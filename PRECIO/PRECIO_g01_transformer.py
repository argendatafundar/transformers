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
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  12 non-null     object 
#   1   valor   12 non-null     float64
#  
#  |    | sector                             |   valor |
#  |---:|:-----------------------------------|--------:|
#  |  0 | Alimentos y bebidas no alcoholicas |   26.93 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  12 non-null     object 
#   1   valor   12 non-null     float64
#  
#  |    | sector                             |   valor |
#  |---:|:-----------------------------------|--------:|
#  |  0 | Alimentos y bebidas no alcoholicas |   26.93 |
#  
#  ------------------------------
#  