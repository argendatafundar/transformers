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
	txtwrapper(col='tipo_trabajo', width=15)
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
#  |  0 | Total  | Doméstico      |       168 |
#  
#  ------------------------------
#  
#  txtwrapper(col='tipo_trabajo', width=15)
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
#  |  0 | Total  | Doméstico      |       168 |
#  
#  ------------------------------
#  