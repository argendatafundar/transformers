from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   geocodigoFundar  24 non-null     object 
#   2   geonombreFundar  24 non-null     object 
#   3   pernoctes        24 non-null     int64  
#   4   share            24 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   pernoctes |   share |
#  |---:|-------:|:------------------|:------------------|------------:|--------:|
#  |  0 |   2024 | AR-B              | Buenos Aires      |     6988105 | 19.5945 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   geocodigoFundar  24 non-null     object 
#   2   geonombreFundar  24 non-null     object 
#   3   pernoctes        24 non-null     int64  
#   4   share            24 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   pernoctes |   share |
#  |---:|-------:|:------------------|:------------------|------------:|--------:|
#  |  0 |   2024 | AR-B              | Buenos Aires      |     6988105 | 19.5945 |
#  
#  ------------------------------
#  