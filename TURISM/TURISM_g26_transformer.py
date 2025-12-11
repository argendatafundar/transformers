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
#  RangeIndex: 68 entries, 0 to 67
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             68 non-null     int64  
#   1   indicador        68 non-null     object 
#   2   expo_turisticas  68 non-null     float64
#  
#  |    |   anio | indicador   |   expo_turisticas |
#  |---:|-------:|:------------|------------------:|
#  |  0 |   1976 | Viajes      |               180 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 68 entries, 0 to 67
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             68 non-null     int64  
#   1   indicador        68 non-null     object 
#   2   expo_turisticas  68 non-null     float64
#  
#  |    |   anio | indicador   |   expo_turisticas |
#  |---:|-------:|:------------|------------------:|
#  |  0 |   1976 | Viajes      |               180 |
#  
#  ------------------------------
#  