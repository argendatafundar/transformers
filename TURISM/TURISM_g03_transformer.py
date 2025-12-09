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
#  RangeIndex: 4861 entries, 0 to 4860
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         4861 non-null   int64  
#   1   geocodigoFundar              4861 non-null   object 
#   2   geonombreFundar              4861 non-null   object 
#   3   int_tourism_arraivals        4823 non-null   float64
#   4   share_int_tourism_arraivals  4823 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   int_tourism_arraivals |   share_int_tourism_arraivals |
#  |---:|-------:|:------------------|:------------------|------------------------:|------------------------------:|
#  |  0 |   1995 | ABW               | Aruba             |                0.618438 |                       0.11416 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 139 entries, 26 to 4860
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         139 non-null    int64  
#   1   geocodigoFundar              139 non-null    object 
#   2   geonombreFundar              139 non-null    object 
#   3   int_tourism_arraivals        131 non-null    float64
#   4   share_int_tourism_arraivals  131 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   int_tourism_arraivals |   share_int_tourism_arraivals |
#  |---:|-------:|:------------------|:------------------|------------------------:|------------------------------:|
#  | 26 |   2024 | ABW               | Aruba             |                     1.4 |                     0.0952508 |
#  
#  ------------------------------
#  