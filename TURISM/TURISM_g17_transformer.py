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
	multiplicar_por_escalar(col='arribos_turistas', k=0.001)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 59 entries, 0 to 58
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              59 non-null     int64  
#   1   geocodigoFundar   59 non-null     object 
#   2   geonombreFundar   59 non-null     object 
#   3   arribos_turistas  59 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turistas |
#  |---:|-------:|:------------------|:------------------|-------------------:|
#  |  0 |   1960 | ARG               | Argentina         |             429319 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='arribos_turistas', k=0.001)
#  RangeIndex: 59 entries, 0 to 58
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              59 non-null     int64  
#   1   geocodigoFundar   59 non-null     object 
#   2   geonombreFundar   59 non-null     object 
#   3   arribos_turistas  59 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turistas |
#  |---:|-------:|:------------------|:------------------|-------------------:|
#  |  0 |   1960 | ARG               | Argentina         |            429.319 |
#  
#  ------------------------------
#  