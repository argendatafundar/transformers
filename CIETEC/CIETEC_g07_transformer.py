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
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  10 non-null     object 
#   1   genero     10 non-null     object 
#   2   cantidad   10 non-null     int64  
#   3   share      10 non-null     float64
#  
#  |    | categoria   | genero   |   cantidad |   share |
#  |---:|:------------|:---------|-----------:|--------:|
#  |  0 | Adjunto     | Mujeres  |       2913 | 58.2367 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  10 non-null     object 
#   1   genero     10 non-null     object 
#   2   cantidad   10 non-null     int64  
#   3   share      10 non-null     float64
#  
#  |    | categoria   | genero   |   cantidad |   share |
#  |---:|:------------|:---------|-----------:|--------:|
#  |  0 | Adjunto     | Mujeres  |       2913 | 58.2367 |
#  
#  ------------------------------
#  