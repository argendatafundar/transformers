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
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente                    |
#  |---:|-------:|------------:|:--------------------------|
#  |  0 |   1820 |      534000 | Maddison Project Database |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente                    |
#  |---:|-------:|------------:|:--------------------------|
#  |  0 |   1820 |      534000 | Maddison Project Database |
#  
#  ------------------------------
#  