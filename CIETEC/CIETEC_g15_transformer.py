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
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   geocodigoFundar             24 non-null     object 
#   1   geonombreFundar             24 non-null     object 
#   2   anio                        24 non-null     int64  
#   3   inversion_10000_hab         24 non-null     float64
#   4   indice_inversion_10000_hab  24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   inversion_10000_hab |   indice_inversion_10000_hab |
#  |---:|:------------------|:------------------|-------:|----------------------:|-----------------------------:|
#  |  0 | AR-B              | Buenos Aires      |   2023 |               223.011 |                      117.661 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   geocodigoFundar             24 non-null     object 
#   1   geonombreFundar             24 non-null     object 
#   2   anio                        24 non-null     int64  
#   3   inversion_10000_hab         24 non-null     float64
#   4   indice_inversion_10000_hab  24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   inversion_10000_hab |   indice_inversion_10000_hab |
#  |---:|:------------------|:------------------|-------:|----------------------:|-----------------------------:|
#  |  0 | AR-B              | Buenos Aires      |   2023 |               223.011 |                      117.661 |
#  
#  ------------------------------
#  