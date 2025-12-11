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
	query(condition='anio in [2000,2010,2024]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7556 entries, 0 to 7555
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7556 non-null   int64  
#   1   geocodigoFundar  7556 non-null   object 
#   2   geonombreFundar  7470 non-null   object 
#   3   expo_turisticas  7556 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   expo_turisticas |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   1986 | ABW               | Aruba             |           158.101 |
#  
#  ------------------------------
#  
#  query(condition='anio in [2000,2010,2024]')
#  Index: 489 entries, 14 to 7555
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             489 non-null    int64  
#   1   geocodigoFundar  489 non-null    object 
#   2   geonombreFundar  481 non-null    object 
#   3   expo_turisticas  489 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   expo_turisticas |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  | 14 |   2000 | ABW               | Aruba             |           814.486 |
#  
#  ------------------------------
#  