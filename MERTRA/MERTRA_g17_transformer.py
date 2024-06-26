from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_trabajo': 'categoria', 'sexo': 'indicador', 'minutos': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   tipo_trabajo  9 non-null      object
#   1   sexo          9 non-null      object
#   2   minutos       9 non-null      int64 
#  
#  |    | tipo_trabajo   | sexo    |   minutos |
#  |---:|:---------------|:--------|----------:|
#  |  0 | No remunerado  | Mujeres |       359 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_trabajo': 'categoria', 'sexo': 'indicador', 'minutos': 'valor'})
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  9 non-null      object
#   1   indicador  9 non-null      object
#   2   valor      9 non-null      int64 
#  
#  |    | categoria     | indicador   |   valor |
#  |---:|:--------------|:------------|--------:|
#  |  0 | No remunerado | Mujeres     |     359 |
#  
#  ------------------------------
#  