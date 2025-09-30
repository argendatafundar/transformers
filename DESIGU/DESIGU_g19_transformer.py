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
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       66 non-null     int64  
#   1   variable  66 non-null     object 
#   2   valor     66 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |   2.057 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       66 non-null     int64  
#   1   variable  66 non-null     object 
#   2   valor     66 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |   2.057 |
#  
#  ------------------------------
#  