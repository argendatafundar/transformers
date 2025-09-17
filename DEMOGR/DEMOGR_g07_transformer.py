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
#  RangeIndex: 78 entries, 0 to 77
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    78 non-null     int64  
#   1   tgf     78 non-null     float64
#   2   fuente  78 non-null     object 
#  
#  |    |   anio |   tgf | fuente   |
#  |---:|-------:|------:|:---------|
#  |  0 |   1869 |   6.8 | INDEC    |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 78 entries, 0 to 77
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    78 non-null     int64  
#   1   tgf     78 non-null     float64
#   2   fuente  78 non-null     object 
#  
#  |    |   anio |   tgf | fuente   |
#  |---:|-------:|------:|:---------|
#  |  0 |   1869 |   6.8 | INDEC    |
#  
#  ------------------------------
#  