from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_multiple_values(df : DataFrame, col:str, replace_mapper:dict) -> DataFrame:
    return df.replace({col : replace_mapper})

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col='produccion_total', axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	multiplicar_por_escalar(col='value', k=1e-06),
	replace_multiple_values(col='variable', replace_mapper={'produccion_acuicola': 'Acuicultura', 'produccion_captura': 'Captura'}),
	sort_values(how='ascending', by=['variable', 'anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 63 entries, 0 to 62
#  Data columns (total 4 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 63 non-null     int64  
#   1   produccion_acuicola  63 non-null     float64
#   2   produccion_captura   63 non-null     float64
#   3   produccion_total     63 non-null     float64
#  
#  |    |   anio |   produccion_acuicola |   produccion_captura |   produccion_total |
#  |---:|-------:|----------------------:|---------------------:|-------------------:|
#  |  0 |   1961 |           2.03605e+06 |          3.94583e+07 |        4.14944e+07 |
#  
#  ------------------------------
#  
#  drop_col(col='produccion_total', axis=1)
#  RangeIndex: 63 entries, 0 to 62
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 63 non-null     int64  
#   1   produccion_acuicola  63 non-null     float64
#   2   produccion_captura   63 non-null     float64
#  
#  |    |   anio |   produccion_acuicola |   produccion_captura |
#  |---:|-------:|----------------------:|---------------------:|
#  |  0 |   1961 |           2.03605e+06 |          3.94583e+07 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      126 non-null    int64  
#   1   variable  126 non-null    object 
#   2   value     126 non-null    float64
#  
#  |    |   anio | variable            |   value |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1961 | produccion_acuicola | 2.03605 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='value', k=1e-06)
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      126 non-null    int64  
#   1   variable  126 non-null    object 
#   2   value     126 non-null    float64
#  
#  |    |   anio | variable            |   value |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1961 | produccion_acuicola | 2.03605 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'produccion_acuicola': 'Acuicultura', 'produccion_captura': 'Captura'})
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      126 non-null    int64  
#   1   variable  126 non-null    object 
#   2   value     126 non-null    float64
#  
#  |    |   anio | variable    |   value |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Acuicultura | 2.03605 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['variable', 'anio'])
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      126 non-null    int64  
#   1   variable  126 non-null    object 
#   2   value     126 non-null    float64
#  
#  |    |   anio | variable    |   value |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Acuicultura | 2.03605 |
#  
#  ------------------------------
#  