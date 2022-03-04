#Importar bibliotecas
import requests
import zipfile

import pandas as pd
import geopandas as gpd
import json
from shapely.geometry import Point, mapping, shape, Polygon, LineString
import folium

from owslib.wfs import WebFeatureService
from geojson import dump

import streamlit as st
import plotly.express as px
from streamlit_folium import folium_static


#Los datos se descargan una vez y se almacenan en Cache
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def descargar_datos():

    url_cantones = 'https://geos.snitcr.go.cr/be/IGN_5/wfs?'
    url_red_vial = 'https://geos.snitcr.go.cr/be/IGN_200/wfs?version=1.1.0'

    # Primero, los límites cantonales
    params_cantones = dict(service='WFS',
                request='GetFeature', 
                typeName='IGN_5:limitecantonal_5k', #Capa de cantones a escala 1:5000
                srsName='urn:ogc:def:crs:EPSG::4326', #Coordenadas en WGS 84 
                outputFormat='json')

    #Se convierte en formato json
    capa_cantones_json = requests.get(url_cantones, params=params_cantones, verify=False).json()

    #Se convierte también en Dataframe
    response_cantones = requests.Request('GET',url_cantones, params=params_cantones).prepare().url
    cantones = gpd.read_file(response_cantones)
    columns_drop_cantones = ['id','gmlid', 'cod_catalo', 'cod_canton', 'ori_toponi', 'cod_provin', 'version']
    cantones = cantones.drop(columns=columns_drop_cantones)

    # Segundo, la red vial
    params_red = dict(service='WFS',
                request='GetFeature', 
                typeName='IGN_200:redvial_200k', #Capa de red vial a escala 1:200 000
                srsName='urn:ogc:def:crs:EPSG::4326', #Coordenadas en WGS 84
                outputFormat='json')

    #Se convierte en formato json
    capa_red_json = requests.get(url_red_vial, params=params_red, verify=False).json()

    #Se convierte también en Dataframe
    response_red = requests.Request('GET',url_red_vial, params=params_red).prepare().url
    red_vial = gpd.read_file(response_red)

    columns_drop_red = ['origen', 'codigo', 'num_ruta', 'jerarquia', 'nombre',
        'num_carril', 'mat_supe', 'est_supe', 'condi_uso', 'administra',
        'fiabilidad', 'num_carr', 'estac_peaj', 'id', 'tipo',
        'et_id', 'et_source', 'fid_', 'entity', 'handle', 'layer', 'lyrfrzn',
        'lyrlock', 'lyron', 'lyrvpfrzn', 'lyrhandle', 'color', 'entcolor',
        'lyrcolor', 'blkcolor', 'linetype', 'entlinetyp', 'lyrlntype',
        'blklinetyp', 'elevation', 'thickness', 'linewt', 'entlinewt',
        'lyrlinewt', 'blklinewt', 'refname', 'ltscale', 'extx', 'exty', 'extz',
        'docname', 'docpath', 'doctype', 'docver']

    red_vial = red_vial.drop(columns=columns_drop_red)

    # Lógica para calcular aquellos tramos de carreteras que en verdad se encuentran dentro de cada cantón.
    lista_rutas_coordenadas = []
    lista_rutas_categorias = []

    for i in range(len(capa_cantones_json["features"])):
        #Iteración por cada cantón
        canton_coordenadas = convertir_coordenadas_tuplas(capa_cantones_json["features"][i]["geometry"]["coordinates"],'canton')
        canton = Polygon(canton_coordenadas)

        for j in range(len(capa_red_json["features"])):  
            #Iteración por cada ruta        
            ruta_coordenadas = convertir_coordenadas_tuplas(capa_red_json["features"][j]["geometry"]["coordinates"],'ruta')
            ruta = LineString(ruta_coordenadas)
            
            if canton.intersects(ruta):
                # intersection proporciona una buena aproximación de la ruta recortada dentro del poligono
                interseccion_resultante = canton.intersection(ruta)
                lista_rutas_coordenadas.append(interseccion_resultante)
                lista_rutas_categorias.append(capa_red_json["features"][j]["properties"]["categoria"])

    # Se convierte la lista en un objeto GeoSeries y luego un GeoDataFrame
    # Luego se agregan al GeoFataFrame la columna de longitud y categoria

    geo_lista = gpd.GeoSeries(lista_rutas_coordenadas)
    geo_data = gpd.GeoDataFrame(geo_lista, columns = ['geometry'])
    geo_data['longitud'] = gpd.GeoSeries(geo_data['geometry']).length*100000
    geo_data['categoria'] = gpd.GeoSeries(lista_rutas_categorias)

    # Se hace un join espacial del DataFrame cantones y geo_data
    # El join conserva todas las rutas "right join"

    join_espacial = gpd.sjoin(cantones,geo_data, how="right")
    longitud_agrupada = join_espacial.groupby('canton')['longitud'].sum()

    #Se calcula la densidad y se agrega como columna

    cantones_sorted = cantones.sort_values(by=['canton'], ascending=True)
    cantones_sorted = cantones_sorted.assign(longitud_total=longitud_agrupada.values/1000)
    cantones_sorted['densidad_total'] = cantones_sorted.apply(lambda row: row.longitud_total / row.area, axis=1)

    longitud_sin_pavimento = join_espacial.query('categoria=="CARRETERA SIN PAVIMENTO DOS VIAS"').groupby('canton')['longitud'].sum()/1000
    longitud_pavimento_1 = join_espacial.query('categoria=="CARRETERA PAVIMENTO UNA VIA"').groupby('canton')['longitud'].sum()/1000
    longitud_pavimento_2 = join_espacial.query('categoria=="CARRETERA PAVIMENTO DOS VIAS O MAS"').groupby('canton')['longitud'].sum()/1000
    longitud_camino_tierra = join_espacial.query('categoria=="CAMINO DE TIERRA"').groupby('canton')['longitud'].sum()/1000
    longitud_autopista = join_espacial.query('categoria=="AUTOPISTA"').groupby('canton')['longitud'].sum()/1000

    # Se convierten estas longitudes en DataFrame

    data_longitud_sin_pavimento = pd.DataFrame({'canton':longitud_sin_pavimento.index,
                                              'longitud_sin_pavimento':longitud_sin_pavimento.values})
    data_longitud_pavimento_1 = pd.DataFrame({'canton':longitud_pavimento_1.index,
                                              'longitud_pavimento_1':longitud_pavimento_1.values})
    data_longitud_pavimento_2 = pd.DataFrame({'canton':longitud_pavimento_2.index,
                                              'longitud_pavimento_2':longitud_pavimento_2.values})
    data_longitud_camino_tierra = pd.DataFrame({'canton':longitud_camino_tierra.index,
                                              'longitud_camino_tierra':longitud_camino_tierra.values})
    data_longitud_autopista = pd.DataFrame({'canton':longitud_autopista.index,
                                              'longitud_autopista':longitud_autopista.values})

    return cantones_sorted, data_longitud_sin_pavimento, data_longitud_pavimento_1, data_longitud_pavimento_2, data_longitud_camino_tierra, data_longitud_autopista, capa_red_json, join_espacial

def convertir_coordenadas_tuplas(coordenadas,tipo):
    len_coordenadas = len(coordenadas)
    
    if(tipo=='canton'):
        if(len_coordenadas==1):
            for i in coordenadas:
                coordenadas_tuplas = [tuple(j) for j in i]
        else:
            for i in coordenadas:
                for j in i:
                    coordenadas_tuplas = [tuple(k) for k in j]
    else:  
        coordenadas_tuplas = [tuple(i) for i in coordenadas]
        
    return coordenadas_tuplas


#Se cargan todos los DataFrames
cantones_sorted, data_longitud_sin_pavimento, data_longitud_pavimento_1, data_longitud_pavimento_2, data_longitud_camino_tierra, data_longitud_autopista , capa_red_json, join_espacial = descargar_datos()

# Se crea una copia a sugerencia de Streamlit. De lo contrario el Cache no funciona.
cantones_stream = cantones_sorted.copy()
cantones_stream.drop(columns = ['geometry', 'provincia', 'densidad_total'], inplace=True)

# Inicia Streamlit aqui
st.title("Proyecto del curso de laboratorio")
st.markdown("# Luis Sanchez - A65285")

#1. Sidebar
carreteras_seleccionadas = st.sidebar.selectbox("Por favor seleccione los tipos de carretera de los que desea obtener informacion",
("Sin pavimento de dos vías", "De pavimento de una vía", "De pavimento de dos vías o más", "Caminos de tierra", "Autopistas", "Todos los tipos de carretera"))

st.markdown("## 1. Categoria seleccionada: "+str(carreteras_seleccionadas))

# Diccionarios para facilitar el trabajo de conversion de la opcion seleccionada
diccionario_carreteras_keys = {"Sin pavimento de dos vías":"longitud_sin_pavimento",
    "De pavimento de una vía":"longitud_pavimento_1",
    "De pavimento de dos vías o más":"longitud_pavimento_2",
    "Caminos de tierra":"longitud_camino_tierra",
    "Autopistas":"longitud_autopista",
    "Todos los tipos de carretera":"longitud_total"}

todas_carreteras = "Todos los tipos de carretera"

diccionario_carreteras_folium = {"Sin pavimento de dos vías":"CARRETERA SIN PAVIMENTO DOS VIAS",
    "De pavimento de una vía":"CARRETERA PAVIMENTO UNA VIA",
    "De pavimento de dos vías o más":"CARRETERA PAVIMENTO DOS VIAS O MAS",
    "Caminos de tierra":"CAMINO DE TIERRA",
    "Autopistas":"AUTOPISTA"}

diccionario_carreteras_datos = {"Sin pavimento de dos vías":data_longitud_sin_pavimento,
    "De pavimento de una vía":data_longitud_pavimento_1,
    "De pavimento de dos vías o más":data_longitud_pavimento_2,
    "Caminos de tierra":data_longitud_camino_tierra,
    "Autopistas":data_longitud_autopista}

#Se arreglan valores nulos
if(carreteras_seleccionadas != todas_carreteras):
    cantones_stream.drop(columns = ['longitud_total'], inplace=True)
    cantones_stream = cantones_stream.merge(diccionario_carreteras_datos[carreteras_seleccionadas], on='canton', how='left')
    cantones_stream.fillna(0, inplace=True)
    red_vial_categoria = join_espacial.loc[join_espacial['categoria'] == diccionario_carreteras_folium[carreteras_seleccionadas]]
else:
    red_vial_categoria = capa_red_json

# 2. Tabla
# Hay que calcular la densidad para el tipo de carretera seleccionada
cantones_tabla = cantones_stream.rename(columns={diccionario_carreteras_keys[carreteras_seleccionadas]:'longitud_total'})
cantones_tabla['densidad_total'] = cantones_tabla.apply(lambda row: row.longitud_total / row.area, axis=1)

st.markdown("## 2. Tabla con información de cantones y rutas")

st.table(data=cantones_tabla)

# 3. Grafico de barras Plotly
cantones_top_15 = cantones_stream.nlargest(n=15, columns=[diccionario_carreteras_keys[carreteras_seleccionadas]])
st.markdown("## 3. Gráfico de barras Plotly con el top 15 del tipo de carretera seleccionado")

fig_bar = px.bar(cantones_top_15,
    x="canton",
    y=diccionario_carreteras_keys[carreteras_seleccionadas],
    labels={
        "value": "Distancia (km)",
        "canton": "Cantones",
        "variable":"Tipo de carretera"
    },
    title="Distribución por tipo de carretera en los 15 cantones con más longitud total")

st.plotly_chart(fig_bar)

# 4. Grafico de pastel Plotly
st.markdown("## 4. Gráfico de pastel Plotly con el top 15 del tipo de carretera seleccionada y otros cantones")

otros_cantones = {'canton':'Otros cantones', 
                    diccionario_carreteras_keys[carreteras_seleccionadas]:cantones_stream[diccionario_carreteras_keys[carreteras_seleccionadas]].sum()-cantones_top_15[diccionario_carreteras_keys[carreteras_seleccionadas]].sum()}
cantones_top_16 = cantones_top_15.append(otros_cantones, ignore_index=True)
fig_pie = px.pie(cantones_top_16, values=diccionario_carreteras_keys[carreteras_seleccionadas], names='canton',
            title='Proporción de los 15 cantones con mayor longitud del tipo de carreta seleccionada y "otros cantones"')

st.plotly_chart(fig_pie)

# 5. Mapa folium

# Originalmente pense en crear un Cache del mapa, pero no se recomienda copiar este objeto
# referencia: https://github.com/python-visualization/folium/issues/1207

# Ademas, la prueba resultó con tiempos de carga similares
m = folium.Map(location=[9.8, -84], tiles='CartoDB positron', zoom_start=8, control_scale=True)

st.markdown("## 5. Mapa folium con la información de la carretera seleccionada")



folium.Choropleth(
    name="Densidad vial de "+str(diccionario_carreteras_keys[carreteras_seleccionadas]),
    geo_data=cantones_sorted,
    data=cantones_tabla,
    columns=["canton","densidad_total"],
    bins=7,
    key_on="feature.properties.canton",
    fill_color="Reds",
    fill_opacity=0.5, 
    line_opacity=1,
    legend_name="Densidad (Longitud/Area)",
).add_to(m)

folium.GeoJson(data=red_vial_categoria,
               name='Red vial por categoría'
              ).add_to(m)

folium.LayerControl().add_to(m)

folium_static(m)
