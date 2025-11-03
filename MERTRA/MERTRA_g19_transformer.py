from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def txtwrapper(df: DataFrame, col:str, width:int)->DataFrame:
    import textwrap
    df[col] = df[col].apply(lambda text: "\n".join(textwrap.wrap(text, width=width)))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	txtwrapper(col='nivel_educativo', width=15)
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
#  txtwrapper(col='nivel_educativo', width=15)
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