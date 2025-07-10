from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   81 non-null     object
#   1   geonombreFundar   81 non-null     object
#   2   anio              81 non-null     int64 
#   3   funcion           81 non-null     object
#   4   personas_fisicas  81 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   2017 | Investigadores |              84284 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   81 non-null     object
#   1   geonombreFundar   81 non-null     object
#   2   anio              81 non-null     int64 
#   3   funcion           81 non-null     object
#   4   personas_fisicas  81 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   2017 | Investigadores |              84284 |
#  
#  ------------------------------
#  