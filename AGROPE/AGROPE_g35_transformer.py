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
#  RangeIndex: 720 entries, 0 to 719
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  720 non-null    object 
#   1   geonombreFundar  720 non-null    object 
#   2   cadena           720 non-null    object 
#   3   share            720 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | cadena         |   share |
#  |---:|:------------------|:------------------|:---------------|--------:|
#  |  0 | AR-B              | Buenos Aires      | Algodón textil |   36.64 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 720 entries, 0 to 719
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  720 non-null    object 
#   1   geonombreFundar  720 non-null    object 
#   2   cadena           720 non-null    object 
#   3   share            720 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | cadena         |   share |
#  |---:|:------------------|:------------------|:---------------|--------:|
#  |  0 | AR-B              | Buenos Aires      | Algodón textil |   36.64 |
#  
#  ------------------------------
#  