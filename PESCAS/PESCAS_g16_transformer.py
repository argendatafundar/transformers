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
	replace_multiple_values(col='variable', replace_mapper={'fob_index': 'Valor', 'toneladas_index': 'Cantidad', 'precio_index': 'Precio'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             23 non-null     int64  
#   1   fob_index        23 non-null     float64
#   2   toneladas_index  23 non-null     float64
#   3   precio_index     23 non-null     float64
#  
#  |    |   anio |   fob_index |   toneladas_index |   precio_index |
#  |---:|-------:|------------:|------------------:|---------------:|
#  |  0 |   2002 |         100 |               100 |            100 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      69 non-null     int64  
#   1   variable  69 non-null     object 
#   2   value     69 non-null     float64
#  
#  |    |   anio | variable   |   value |
#  |---:|-------:|:-----------|--------:|
#  |  0 |   2002 | fob_index  |     100 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'fob_index': 'Valor', 'toneladas_index': 'Cantidad', 'precio_index': 'Precio'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      69 non-null     int64  
#   1   variable  69 non-null     object 
#   2   value     69 non-null     float64
#  
#  |    |   anio | variable   |   value |
#  |---:|-------:|:-----------|--------:|
#  |  0 |   2002 | Valor      |     100 |
#  
#  ------------------------------
#  