from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="flujo != 'balanza'"),
	replace_multiple_values(col='flujo', replacements={'expo_turistica': 'Exportaciones', 'impo_turistica': 'Importaciones'}),
	multiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 147 entries, 0 to 146
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    147 non-null    int64  
#   1   flujo   147 non-null    object 
#   2   valor   147 non-null    float64
#  
#  |    |   anio | flujo          |   valor |
#  |---:|-------:|:---------------|--------:|
#  |  0 |   1976 | expo_turistica | 1.8e+08 |
#  
#  ------------------------------
#  
#  query(condition="flujo != 'balanza'")
#  Index: 98 entries, 0 to 145
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    98 non-null     int64  
#   1   flujo   98 non-null     object 
#   2   valor   98 non-null     float64
#  
#  |    |   anio | flujo          |   valor |
#  |---:|-------:|:---------------|--------:|
#  |  0 |   1976 | expo_turistica | 1.8e+08 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='flujo', replacements={'expo_turistica': 'Exportaciones', 'impo_turistica': 'Importaciones'})
#  Index: 98 entries, 0 to 145
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    98 non-null     int64  
#   1   flujo   98 non-null     object 
#   2   valor   98 non-null     float64
#  
#  |    |   anio | flujo         |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1976 | Exportaciones |     180 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  Index: 98 entries, 0 to 145
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    98 non-null     int64  
#   1   flujo   98 non-null     object 
#   2   valor   98 non-null     float64
#  
#  |    |   anio | flujo         |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1976 | Exportaciones |     180 |
#  
#  ------------------------------
#  