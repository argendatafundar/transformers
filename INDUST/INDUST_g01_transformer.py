from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col='geocodigoFundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10930 entries, 0 to 10929
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  10930 non-null  object 
#   1   geonombreFundar  10930 non-null  object 
#   2   anio             10930 non-null  int64  
#   3   gdp_indust_pc    10930 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gdp_indust_pc |
#  |---:|:------------------|:------------------|-------:|----------------:|
#  |  0 | AFG               | Afganistán        |   1970 |         33.1151 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 212 entries, 53 to 10929
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  212 non-null    object 
#   1   geonombreFundar  212 non-null    object 
#   2   anio             212 non-null    int64  
#   3   gdp_indust_pc    212 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gdp_indust_pc |
#  |---:|:------------------|:------------------|-------:|----------------:|
#  | 53 | AFG               | Afganistán        |   2023 |         31.8455 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 212 entries, 53 to 10929
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  212 non-null    object 
#   1   anio             212 non-null    int64  
#   2   gdp_indust_pc    212 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   gdp_indust_pc |
#  |---:|:------------------|-------:|----------------:|
#  | 53 | Afganistán        |   2023 |         31.8455 |
#  
#  ------------------------------
#  