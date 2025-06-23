from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	replace_value(col='variable', curr_value='share_bienes', new_value='Bienes'),
	replace_value(col='variable', curr_value='share_expo', new_value='Bienes y servicios'),
	drop_na(col='value')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          62 non-null     int64  
#   1   share_bienes  62 non-null     float64
#   2   share_expo    44 non-null     float64
#  
#  |    |   anio |   share_bienes |   share_expo |
#  |---:|-------:|---------------:|-------------:|
#  |  0 |   1962 |      0.0190338 |          nan |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      124 non-null    int64  
#   1   variable  124 non-null    object 
#   2   value     106 non-null    float64
#  
#  |    |   anio | variable     |     value |
#  |---:|-------:|:-------------|----------:|
#  |  0 |   1962 | share_bienes | 0.0190338 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='share_bienes', new_value='Bienes')
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      124 non-null    int64  
#   1   variable  124 non-null    object 
#   2   value     106 non-null    float64
#  
#  |    |   anio | variable   |     value |
#  |---:|-------:|:-----------|----------:|
#  |  0 |   1962 | Bienes     | 0.0190338 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='share_expo', new_value='Bienes y servicios')
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      124 non-null    int64  
#   1   variable  124 non-null    object 
#   2   value     106 non-null    float64
#  
#  |    |   anio | variable   |     value |
#  |---:|-------:|:-----------|----------:|
#  |  0 |   1962 | Bienes     | 0.0190338 |
#  
#  ------------------------------
#  
#  drop_na(col='value')
#  Index: 106 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      106 non-null    int64  
#   1   variable  106 non-null    object 
#   2   value     106 non-null    float64
#  
#  |    |   anio | variable   |     value |
#  |---:|-------:|:-----------|----------:|
#  |  0 |   1962 | Bienes     | 0.0190338 |
#  
#  ------------------------------
#  