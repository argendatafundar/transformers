from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def replace_multiple_values(df : DataFrame, col:str, replace_mapper:dict) -> DataFrame:
    return df.replace({col : replace_mapper})

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['saldo_comercial_minero'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	multiplicar_por_escalar(col='value', k=1e-06),
	replace_multiple_values(col='variable', replace_mapper={'exportaciones_mineras': 'Exportaciones', 'importaciones_mineras': 'Importaciones'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    29 non-null     int64  
#   1   exportaciones_mineras   29 non-null     float64
#   2   importaciones_mineras   29 non-null     float64
#   3   saldo_comercial_minero  29 non-null     float64
#  
#  |    |   anio |   exportaciones_mineras |   importaciones_mineras |   saldo_comercial_minero |
#  |---:|-------:|------------------------:|------------------------:|-------------------------:|
#  |  0 |   1994 |             6.56879e+07 |             3.53802e+08 |             -2.88114e+08 |
#  
#  ------------------------------
#  
#  drop_col(col=['saldo_comercial_minero'], axis=1)
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   29 non-null     int64  
#   1   exportaciones_mineras  29 non-null     float64
#   2   importaciones_mineras  29 non-null     float64
#  
#  |    |   anio |   exportaciones_mineras |   importaciones_mineras |
#  |---:|-------:|------------------------:|------------------------:|
#  |  0 |   1994 |             6.56879e+07 |             3.53802e+08 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 58 entries, 0 to 57
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      58 non-null     int64  
#   1   variable  58 non-null     object 
#   2   value     58 non-null     float64
#  
#  |    |   anio | variable              |   value |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1994 | exportaciones_mineras | 65.6879 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='value', k=1e-06)
#  RangeIndex: 58 entries, 0 to 57
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      58 non-null     int64  
#   1   variable  58 non-null     object 
#   2   value     58 non-null     float64
#  
#  |    |   anio | variable              |   value |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1994 | exportaciones_mineras | 65.6879 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'exportaciones_mineras': 'Exportaciones', 'importaciones_mineras': 'Importaciones'})
#  RangeIndex: 58 entries, 0 to 57
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      58 non-null     int64  
#   1   variable  58 non-null     object 
#   2   value     58 non-null     float64
#  
#  |    |   anio | variable      |   value |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1994 | Exportaciones | 65.6879 |
#  
#  ------------------------------
#  