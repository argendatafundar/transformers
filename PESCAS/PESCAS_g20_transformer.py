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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	multiplicar_por_escalar(col='value', k=0.001),
	replace_multiple_values(col='variable', replace_mapper={'desembarque_toneladas': 'Capturas de Merluza Hubbsi', 'cmp_total': 'Captura Máxima Permisible'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   36 non-null     int64  
#   1   desembarque_toneladas  36 non-null     float64
#   2   cmp_total              15 non-null     float64
#  
#  |    |   anio |   desembarque_toneladas |   cmp_total |
#  |---:|-------:|------------------------:|------------:|
#  |  0 |   1989 |                  298684 |         nan |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 72 entries, 0 to 71
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      72 non-null     int64  
#   1   variable  72 non-null     object 
#   2   value     51 non-null     float64
#  
#  |    |   anio | variable              |   value |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1989 | desembarque_toneladas | 298.684 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='value', k=0.001)
#  RangeIndex: 72 entries, 0 to 71
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      72 non-null     int64  
#   1   variable  72 non-null     object 
#   2   value     51 non-null     float64
#  
#  |    |   anio | variable              |   value |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1989 | desembarque_toneladas | 298.684 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'desembarque_toneladas': 'Capturas de Merluza Hubbsi', 'cmp_total': 'Captura Máxima Permisible'})
#  RangeIndex: 72 entries, 0 to 71
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      72 non-null     int64  
#   1   variable  72 non-null     object 
#   2   value     51 non-null     float64
#  
#  |    |   anio | variable                   |   value |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1989 | Capturas de Merluza Hubbsi | 298.684 |
#  
#  ------------------------------
#  