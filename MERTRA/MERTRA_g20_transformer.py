from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sexo': 'indicador', 'tipo_trabajo': 'categoria', 'minutos': 'valor'}),
	query(condition="indicador != 'Total'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   tipo_trabajo  9 non-null      object 
#   1   sexo          9 non-null      object 
#   2   minutos       9 non-null      float64
#  
#  |    | tipo_trabajo                                          | sexo    |   minutos |
#  |---:|:------------------------------------------------------|:--------|----------:|
#  |  0 | Apoyo a otros hogares, para la comunidad y voluntario | Mujeres |    22.134 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'indicador', 'tipo_trabajo': 'categoria', 'minutos': 'valor'})
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  9 non-null      object 
#   1   indicador  9 non-null      object 
#   2   valor      9 non-null      float64
#  
#  |    | categoria                                             | indicador   |   valor |
#  |---:|:------------------------------------------------------|:------------|--------:|
#  |  0 | Apoyo a otros hogares, para la comunidad y voluntario | Mujeres     |  22.134 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 6 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  6 non-null      object 
#   1   indicador  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |    | categoria                                             | indicador   |   valor |
#  |---:|:------------------------------------------------------|:------------|--------:|
#  |  0 | Apoyo a otros hogares, para la comunidad y voluntario | Mujeres     |  22.134 |
#  
#  ------------------------------
#  