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
#  RangeIndex: 74 entries, 0 to 73
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             74 non-null     int64  
#   1   imr              74 non-null     float64
#   2   prob_sobrevivir  74 non-null     float64
#  
#  |    |   anio |     imr |   prob_sobrevivir |
#  |---:|-------:|--------:|------------------:|
#  |  0 |   1950 | 71.9957 |           1.26862 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 74 entries, 0 to 73
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             74 non-null     int64  
#   1   imr              74 non-null     float64
#   2   prob_sobrevivir  74 non-null     float64
#  
#  |    |   anio |     imr |   prob_sobrevivir |
#  |---:|-------:|--------:|------------------:|
#  |  0 |   1950 | 71.9957 |           1.26862 |
#  
#  ------------------------------
#  