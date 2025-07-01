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
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            89 non-null     int64  
#   1   tasa_inflacion  89 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            89 non-null     int64  
#   1   tasa_inflacion  89 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  