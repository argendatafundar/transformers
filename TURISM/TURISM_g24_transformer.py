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
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     88 non-null     int64  
#   1   variable                 88 non-null     object 
#   2   share_turismo_receptivo  88 non-null     float64
#  
#  |    |   anio | variable                                                   |   share_turismo_receptivo |
#  |---:|-------:|:-----------------------------------------------------------|--------------------------:|
#  |  0 |   1960 | Turismo receptivo en Argentina, como % del turismo mundial |                  0.617284 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     88 non-null     int64  
#   1   variable                 88 non-null     object 
#   2   share_turismo_receptivo  88 non-null     float64
#  
#  |    |   anio | variable                                                   |   share_turismo_receptivo |
#  |---:|-------:|:-----------------------------------------------------------|--------------------------:|
#  |  0 |   1960 | Turismo receptivo en Argentina, como % del turismo mundial |                  0.617284 |
#  
#  ------------------------------
#  