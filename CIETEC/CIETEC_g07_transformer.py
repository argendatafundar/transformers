from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='genero', replacements={'Varones': 'Los Varones', 'Mujeres': 'Las Mujeres'}),
	ordenar_dos_columnas(col1='categoria', order1=['Asistente', 'Adjunto', 'Independiente', 'Principal', 'Superior'], col2='genero', order2=['Las Mujeres', 'Los Varones'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  10 non-null     object 
#   1   genero     10 non-null     object 
#   2   cantidad   10 non-null     int64  
#   3   share      10 non-null     float64
#  
#  |    | categoria   | genero   |   cantidad |   share |
#  |---:|:------------|:---------|-----------:|--------:|
#  |  0 | Adjunto     | Mujeres  |       2913 | 58.2367 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='genero', replacements={'Varones': 'Los Varones', 'Mujeres': 'Las Mujeres'})
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  10 non-null     category
#   1   genero     10 non-null     category
#   2   cantidad   10 non-null     int64   
#   3   share      10 non-null     float64 
#  
#  |    | categoria   | genero      |   cantidad |   share |
#  |---:|:------------|:------------|-----------:|--------:|
#  |  0 | Adjunto     | Las Mujeres |       2913 | 58.2367 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='categoria', order1=['Asistente', 'Adjunto', 'Independiente', 'Principal', 'Superior'], col2='genero', order2=['Las Mujeres', 'Los Varones'])
#  Index: 10 entries, 2 to 9
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  10 non-null     category
#   1   genero     10 non-null     category
#   2   cantidad   10 non-null     int64   
#   3   share      10 non-null     float64 
#  
#  |    | categoria   | genero      |   cantidad |   share |
#  |---:|:------------|:------------|-----------:|--------:|
#  |  2 | Asistente   | Las Mujeres |       1805 | 60.3881 |
#  
#  ------------------------------
#  