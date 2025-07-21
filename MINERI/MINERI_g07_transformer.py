from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def replace_multiple_values(df : DataFrame, col:str, replace_mapper:dict) -> DataFrame:
    return df.replace({col : replace_mapper})
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
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
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 87 entries, 0 to 86
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      87 non-null     int64  
#   1   variable  87 non-null     object 
#   2   value     87 non-null     float64
#  
#  |    |   anio | variable              |       value |
#  |---:|-------:|:----------------------|------------:|
#  |  0 |   1994 | exportaciones_mineras | 6.56879e+07 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'exportaciones_mineras': 'Exportaciones', 'importaciones_mineras': 'Importaciones'})
#  RangeIndex: 87 entries, 0 to 86
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      87 non-null     int64  
#   1   variable  87 non-null     object 
#   2   value     87 non-null     float64
#  
#  |    |   anio | variable      |       value |
#  |---:|-------:|:--------------|------------:|
#  |  0 |   1994 | Exportaciones | 6.56879e+07 |
#  
#  ------------------------------
#  