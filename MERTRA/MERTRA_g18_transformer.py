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
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   sexo        12 non-null     object 
#   1   grupo_edad  12 non-null     object 
#   2   minutos     12 non-null     float64
#  
#  |    | sexo   | grupo_edad   |   minutos |
#  |---:|:-------|:-------------|----------:|
#  |  0 | Total  | Total        |   266.419 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   sexo        12 non-null     object 
#   1   grupo_edad  12 non-null     object 
#   2   minutos     12 non-null     float64
#  
#  |    | sexo   | grupo_edad   |   minutos |
#  |---:|:-------|:-------------|----------:|
#  |  0 | Total  | Total        |   266.419 |
#  
#  ------------------------------
#  