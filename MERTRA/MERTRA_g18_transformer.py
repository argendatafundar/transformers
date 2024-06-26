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
rename_cols(map={'sexo': 'indicador', 'grupo_edad': 'categoria', 'minutos': 'valor'}),
	query(condition="indicador != 'Total'")
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
#  |    | sexo    | grupo_edad   |   minutos |
#  |---:|:--------|:-------------|----------:|
#  |  0 | Mujeres | 14 a 29 años |   268.659 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'indicador', 'grupo_edad': 'categoria', 'minutos': 'valor'})
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |    | indicador   | categoria    |   valor |
#  |---:|:------------|:-------------|--------:|
#  |  0 | Mujeres     | 14 a 29 años | 268.659 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 8 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  8 non-null      object 
#   1   categoria  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    | indicador   | categoria    |   valor |
#  |---:|:------------|:-------------|--------:|
#  |  0 | Mujeres     | 14 a 29 años | 268.659 |
#  
#  ------------------------------
#  