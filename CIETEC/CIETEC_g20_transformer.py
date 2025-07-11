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
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              42 non-null     int64  
#   1   funcion           42 non-null     object 
#   2   personas_fisicas  42 non-null     int64  
#   3   share_mujeres     42 non-null     float64
#  
#  |    |   anio | funcion   |   personas_fisicas |   share_mujeres |
#  |---:|-------:|:----------|-------------------:|----------------:|
#  |  0 |   2003 | Becarios  |               4129 |         55.4824 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              42 non-null     int64  
#   1   funcion           42 non-null     object 
#   2   personas_fisicas  42 non-null     int64  
#   3   share_mujeres     42 non-null     float64
#  
#  |    |   anio | funcion   |   personas_fisicas |   share_mujeres |
#  |---:|-------:|:----------|-------------------:|----------------:|
#  |  0 |   2003 | Becarios  |               4129 |         55.4824 |
#  
#  ------------------------------
#  