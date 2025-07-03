from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='province', mapping={'san_luis': 'San Luis', 'neuquen': 'Neuquén', 'santa_fe': 'Santa Fe', 'san_juan': 'San Juan', 'santiago_del_estero': 'Santiago del Estero', 'PBA': 'Buenos Aires', 'caba': 'CABA', 'la_pampa': 'La Pampa', 'tierra_del_fuego': 'Tierra del Fuego', 'salta': 'Salta', 'chubut': 'Chubut', 'cordoba': 'Córdoba', 'tucuman': 'Tucumán', 'jujuy': 'Jujuy', 'chaco': 'Chaco', 'la_rioja': 'La Rioja', 'catamarca': 'Catamarca', 'rio_negro': 'Río Negro', 'mendoza': 'Mendoza', 'formosa': 'Formosa', 'partidos_GBA': 'Partidos del GBA', 'santa_cruz': 'Santa Cruz', 'entre rios': 'Entre Ríos', 'corrientes': 'Corrientes', 'misiones': 'Misiones', 'argentina': 'Total Nacional'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='province', mapping={'san_luis': 'San Luis', 'neuquen': 'Neuquén', 'santa_fe': 'Santa Fe', 'san_juan': 'San Juan', 'santiago_del_estero': 'Santiago del Estero', 'PBA': 'Buenos Aires', 'caba': 'CABA', 'la_pampa': 'La Pampa', 'tierra_del_fuego': 'Tierra del Fuego', 'salta': 'Salta', 'chubut': 'Chubut', 'cordoba': 'Córdoba', 'tucuman': 'Tucumán', 'jujuy': 'Jujuy', 'chaco': 'Chaco', 'la_rioja': 'La Rioja', 'catamarca': 'Catamarca', 'rio_negro': 'Río Negro', 'mendoza': 'Mendoza', 'formosa': 'Formosa', 'partidos_GBA': 'Partidos del GBA', 'santa_cruz': 'Santa Cruz', 'entre rios': 'Entre Ríos', 'corrientes': 'Corrientes', 'misiones': 'Misiones', 'argentina': 'Total Nacional'}, alias=None)
#  
#  ------------------------------
#  