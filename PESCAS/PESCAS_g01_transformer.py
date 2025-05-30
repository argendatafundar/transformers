from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='participacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 239 entries, 0 to 238
#  Data columns (total 7 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 239 non-null    int64  
#   1   iso3                 239 non-null    object 
#   2   pais_nombre          239 non-null    object 
#   3   produccion_captura   239 non-null    float64
#   4   produccion_acuicola  239 non-null    float64
#   5   produccion_total     239 non-null    float64
#   6   participacion        239 non-null    float64
#  
#  |    |   anio | iso3   | pais_nombre   |   produccion_captura |   produccion_acuicola |   produccion_total |   participacion |
#  |---:|-------:|:-------|:--------------|---------------------:|----------------------:|-------------------:|----------------:|
#  |  0 |   2023 | ABW    | Aruba         |                  170 |                   1.5 |              171.5 |     7.49418e-07 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion', k=100)
#  RangeIndex: 239 entries, 0 to 238
#  Data columns (total 7 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 239 non-null    int64  
#   1   iso3                 239 non-null    object 
#   2   pais_nombre          239 non-null    object 
#   3   produccion_captura   239 non-null    float64
#   4   produccion_acuicola  239 non-null    float64
#   5   produccion_total     239 non-null    float64
#   6   participacion        239 non-null    float64
#  
#  |    |   anio | iso3   | pais_nombre   |   produccion_captura |   produccion_acuicola |   produccion_total |   participacion |
#  |---:|-------:|:-------|:--------------|---------------------:|----------------------:|-------------------:|----------------:|
#  |  0 |   2023 | ABW    | Aruba         |                  170 |                   1.5 |              171.5 |     7.49418e-05 |
#  
#  ------------------------------
#  