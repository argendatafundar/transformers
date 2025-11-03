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
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   sexo             15 non-null     object 
#   1   nivel_educativo  15 non-null     object 
#   2   minutos          15 non-null     float64
#  
#  |    | sexo   | nivel_educativo   |   minutos |
#  |---:|:-------|:------------------|----------:|
#  |  0 | Total  | Total             |   266.419 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   sexo             15 non-null     object 
#   1   nivel_educativo  15 non-null     object 
#   2   minutos          15 non-null     float64
#  
#  |    | sexo   | nivel_educativo   |   minutos |
#  |---:|:-------|:------------------|----------:|
#  |  0 | Total  | Total             |   266.419 |
#  
#  ------------------------------
#  