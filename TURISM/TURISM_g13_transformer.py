from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	pivot_longer(id_cols=['anio', 'rama_etq'], names_to_col='variable', values_to_col='value'),
	replace_multiple_values(col='variable', replacements={'fem_tasa': 'Feminización', 'joven_tasa': 'Empleo joven', 'inform_tasa': 'Informalidad'}),
	sort_values(how='descending', by=['variable', 'rama_etq'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 72 entries, 0 to 71
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         72 non-null     int64  
#   1   rama_etq     72 non-null     object 
#   2   fem_tasa     72 non-null     float64
#   3   joven_tasa   72 non-null     float64
#   4   inform_tasa  72 non-null     float64
#  
#  |    |   anio | rama_etq          |   fem_tasa |   joven_tasa |   inform_tasa |
#  |---:|-------:|:------------------|-----------:|-------------:|--------------:|
#  |  0 |   2016 | Agencias de viaje |     44.483 |      28.7972 |       26.5607 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 8 entries, 48 to 71
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         8 non-null      int64  
#   1   rama_etq     8 non-null      object 
#   2   fem_tasa     8 non-null      float64
#   3   joven_tasa   8 non-null      float64
#   4   inform_tasa  8 non-null      float64
#  
#  |    |   anio | rama_etq          |   fem_tasa |   joven_tasa |   inform_tasa |
#  |---:|-------:|:------------------|-----------:|-------------:|--------------:|
#  | 48 |   2024 | Agencias de viaje |    57.9139 |      48.7608 |        21.449 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio', 'rama_etq'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      24 non-null     int64  
#   1   rama_etq  24 non-null     object 
#   2   variable  24 non-null     object 
#   3   value     24 non-null     float64
#  
#  |    |   anio | rama_etq          | variable   |   value |
#  |---:|-------:|:------------------|:-----------|--------:|
#  |  0 |   2024 | Agencias de viaje | fem_tasa   | 57.9139 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'fem_tasa': 'Feminización', 'joven_tasa': 'Empleo joven', 'inform_tasa': 'Informalidad'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      24 non-null     int64  
#   1   rama_etq  24 non-null     object 
#   2   variable  24 non-null     object 
#   3   value     24 non-null     float64
#  
#  |    |   anio | rama_etq          | variable     |   value |
#  |---:|-------:|:------------------|:-------------|--------:|
#  |  0 |   2024 | Agencias de viaje | Feminización | 57.9139 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by=['variable', 'rama_etq'])
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      24 non-null     int64  
#   1   rama_etq  24 non-null     object 
#   2   variable  24 non-null     object 
#   3   value     24 non-null     float64
#  
#  |    |   anio | rama_etq                | variable     |   value |
#  |---:|-------:|:------------------------|:-------------|--------:|
#  |  0 |   2024 | Transporte de pasajeros | Informalidad | 41.7234 |
#  
#  ------------------------------
#  