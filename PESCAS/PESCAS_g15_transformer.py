from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	replace_value(col='variable', curr_value='share_bienes', new_value='Bienes'),
	replace_value(col='variable', curr_value='share_expo', new_value='Bienes y servicios')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          48 non-null     int64  
#   1   share_bienes  48 non-null     float64
#   2   share_expo    44 non-null     float64
#  
#  |    |   anio |   share_bienes |   share_expo |
#  |---:|-------:|---------------:|-------------:|
#  |  0 |   1976 |       0.316521 |          nan |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 96 entries, 0 to 95
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      96 non-null     int64  
#   1   variable  96 non-null     object 
#   2   value     92 non-null     float64
#  
#  |    |   anio | variable     |    value |
#  |---:|-------:|:-------------|---------:|
#  |  0 |   1976 | share_bienes | 0.316521 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='share_bienes', new_value='Bienes')
#  RangeIndex: 96 entries, 0 to 95
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      96 non-null     int64  
#   1   variable  96 non-null     object 
#   2   value     92 non-null     float64
#  
#  |    |   anio | variable   |    value |
#  |---:|-------:|:-----------|---------:|
#  |  0 |   1976 | Bienes     | 0.316521 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='share_expo', new_value='Bienes y servicios')
#  RangeIndex: 96 entries, 0 to 95
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      96 non-null     int64  
#   1   variable  96 non-null     object 
#   2   value     92 non-null     float64
#  
#  |    |   anio | variable   |    value |
#  |---:|-------:|:-----------|---------:|
#  |  0 |   1976 | Bienes     | 0.316521 |
#  
#  ------------------------------
#  