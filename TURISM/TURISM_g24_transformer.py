from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='variable', replacements={'Turismo receptivo en Argentina, como % del turismo de las Américas': 'americas', 'Turismo receptivo en Argentina, como % del turismo mundial': 'mundial'}),
	long_to_wide(index='anio', columns='variable', values='share_turismo_receptivo')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     88 non-null     int64  
#   1   variable                 88 non-null     object 
#   2   share_turismo_receptivo  88 non-null     float64
#  
#  |    |   anio | variable                                                   |   share_turismo_receptivo |
#  |---:|-------:|:-----------------------------------------------------------|--------------------------:|
#  |  0 |   1960 | Turismo receptivo en Argentina, como % del turismo mundial |                  0.617284 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'Turismo receptivo en Argentina, como % del turismo de las Américas': 'americas', 'Turismo receptivo en Argentina, como % del turismo mundial': 'mundial'})
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     88 non-null     int64  
#   1   variable                 88 non-null     object 
#   2   share_turismo_receptivo  88 non-null     float64
#  
#  |    |   anio | variable   |   share_turismo_receptivo |
#  |---:|-------:|:-----------|--------------------------:|
#  |  0 |   1960 | mundial    |                  0.617284 |
#  
#  ------------------------------
#  
#  long_to_wide(index='anio', columns='variable', values='share_turismo_receptivo')
#  RangeIndex: 44 entries, 0 to 43
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      44 non-null     int64  
#   1   americas  44 non-null     float64
#   2   mundial   44 non-null     float64
#  
#  |    |   anio |   americas |   mundial |
#  |---:|-------:|-----------:|----------:|
#  |  0 |   1960 |    2.54795 |  0.617284 |
#  
#  ------------------------------
#  