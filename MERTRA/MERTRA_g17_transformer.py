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
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   sexo          9 non-null      object 
#   1   tipo_trabajo  9 non-null      object 
#   2   minutos       9 non-null      float64
#  
#  |    | sexo   | tipo_trabajo   |   minutos |
#  |---:|:-------|:---------------|----------:|
#  |  0 | Total  | Trabajo total  |   501.643 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   sexo          9 non-null      object 
#   1   tipo_trabajo  9 non-null      object 
#   2   minutos       9 non-null      float64
#  
#  |    | sexo   | tipo_trabajo   |   minutos |
#  |---:|:-------|:---------------|----------:|
#  |  0 | Total  | Trabajo total  |   501.643 |
#  
#  ------------------------------
#  