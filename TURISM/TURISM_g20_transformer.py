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
#  RangeIndex: 49 entries, 0 to 48
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            49 non-null     int64  
#   1   expo_turistica  49 non-null     float64
#   2   prop_expo       49 non-null     float64
#  
#  |    |   anio |   expo_turistica |   prop_expo |
#  |---:|-------:|-----------------:|------------:|
#  |  0 |   1976 |          1.8e+08 |     3.88601 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 49 entries, 0 to 48
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            49 non-null     int64  
#   1   expo_turistica  49 non-null     float64
#   2   prop_expo       49 non-null     float64
#  
#  |    |   anio |   expo_turistica |   prop_expo |
#  |---:|-------:|-----------------:|------------:|
#  |  0 |   1976 |          1.8e+08 |     3.88601 |
#  
#  ------------------------------
#  