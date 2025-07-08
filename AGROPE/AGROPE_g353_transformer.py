from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='cadena', curr_value='Sojera', new_value='Soja'),
	query(condition="cadena == 'Soja'")
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
#  replace_value(col='cadena', curr_value='Sojera', new_value='Soja')
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
#  query(condition="cadena == 'Soja'")
#  Index: 24 entries, 15 to 705
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  24 non-null     object 
#   2   cadena           24 non-null     object 
#   3   share            24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | cadena   |   share |
#  |---:|:------------------|:------------------|:---------|--------:|
#  | 15 | AR-B              | Buenos Aires      | Soja     |   16.35 |
#  
#  ------------------------------
#  