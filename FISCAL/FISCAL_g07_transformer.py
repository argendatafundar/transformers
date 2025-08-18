from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 35 entries, 0 to 34
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigoFundar       35 non-null     object 
#   1   geonombreFundar       35 non-null     object 
#   2   anio                  35 non-null     int64  
#   3   ratio_centralizacion  35 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   ratio_centralizacion |
#  |---:|:------------------|:------------------|-------:|-----------------------:|
#  |  0 | CRI               | Costa Rica        |   2023 |                95.8218 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 35 entries, 0 to 34
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigoFundar       35 non-null     object 
#   1   geonombreFundar       35 non-null     object 
#   2   anio                  35 non-null     int64  
#   3   ratio_centralizacion  35 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   ratio_centralizacion |
#  |---:|:------------------|:------------------|-------:|-----------------------:|
#  |  0 | CRI               | Costa Rica        |   2023 |                95.8218 |
#  
#  ------------------------------
#  