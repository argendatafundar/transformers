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
rename_cols(map={'sexo': 'indicador', 'nivel_educativo': 'cateoria', 'minutos': 'valor'}),
	query(condition="indicador != 'Total'")
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
#  |    | sexo    | nivel_educativo           |   minutos |
#  |---:|:--------|:--------------------------|----------:|
#  |  0 | Mujeres | Hasta primario incompleto |   410.242 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'indicador', 'nivel_educativo': 'cateoria', 'minutos': 'valor'})
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  15 non-null     object 
#   1   cateoria   15 non-null     object 
#   2   valor      15 non-null     float64
#  
#  |    | indicador   | cateoria                  |   valor |
#  |---:|:------------|:--------------------------|--------:|
#  |  0 | Mujeres     | Hasta primario incompleto | 410.242 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 10 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  10 non-null     object 
#   1   cateoria   10 non-null     object 
#   2   valor      10 non-null     float64
#  
#  |    | indicador   | cateoria                  |   valor |
#  |---:|:------------|:--------------------------|--------:|
#  |  0 | Mujeres     | Hasta primario incompleto | 410.242 |
#  
#  ------------------------------
#  