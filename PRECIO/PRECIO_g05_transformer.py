from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col='geocodigoFundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3168 entries, 0 to 3167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3168 non-null   object 
#   1   geonombreFundar  3168 non-null   object 
#   2   anio             3168 non-null   int64  
#   3   inflacion        3168 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   inflacion |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | ARG               | Argentina         |   2000 |   -0.733699 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  RangeIndex: 3168 entries, 0 to 3167
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  3168 non-null   object 
#   1   anio             3168 non-null   int64  
#   2   inflacion        3168 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   inflacion |
#  |---:|:------------------|-------:|------------:|
#  |  0 | Argentina         |   2000 |   -0.733699 |
#  
#  ------------------------------
#  