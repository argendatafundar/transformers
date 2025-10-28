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
	query(condition='anio == 2021')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 576 entries, 0 to 575
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             576 non-null    int64  
#   1   geocodigoFundar  576 non-null    object 
#   2   geonombreFundar  576 non-null    object 
#   3   share_indust_id  576 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_indust_id |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   2011 | ISL               | Islandia          |           44.0774 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021')
#  Index: 37 entries, 6 to 574
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             37 non-null     int64  
#   1   geocodigoFundar  37 non-null     object 
#   2   geonombreFundar  37 non-null     object 
#   3   share_indust_id  37 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_indust_id |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  6 |   2021 | USA               | Estados Unidos    |           54.4896 |
#  
#  ------------------------------
#  