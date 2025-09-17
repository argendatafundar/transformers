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
#  RangeIndex: 77 entries, 0 to 76
#  Data columns (total 2 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              77 non-null     int64  
#   1   tasa_dependencia  77 non-null     float64
#  
#  |    |   anio |   tasa_dependencia |
#  |---:|-------:|-------------------:|
#  |  0 |   2025 |            71.3726 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 77 entries, 0 to 76
#  Data columns (total 2 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              77 non-null     int64  
#   1   tasa_dependencia  77 non-null     float64
#  
#  |    |   anio |   tasa_dependencia |
#  |---:|-------:|-------------------:|
#  |  0 |   2025 |            71.3726 |
#  
#  ------------------------------
#  