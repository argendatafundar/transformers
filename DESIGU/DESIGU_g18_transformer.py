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
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       64 non-null     int64  
#   1   variable  64 non-null     object 
#   2   valor     64 non-null     float64
#  
#  |    |   ano | variable   |    valor |
#  |---:|------:|:-----------|---------:|
#  |  0 |  1992 | cv         | 0.264629 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       64 non-null     int64  
#   1   variable  64 non-null     object 
#   2   valor     64 non-null     float64
#  
#  |    |   ano | variable   |    valor |
#  |---:|------:|:-----------|---------:|
#  |  0 |  1992 | cv         | 0.264629 |
#  
#  ------------------------------
#  