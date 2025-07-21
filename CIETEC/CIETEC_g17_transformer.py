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

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='medida', values_to_col='valor'),
	drop_na(col='valor'),
	replace_value(col='medida', curr_value='id_pib', new_value='I+D'),
	replace_value(col='medida', curr_value='act_pib', new_value='Act. científico tecnológicas')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 34 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     34 non-null     int64  
#   1   id_pib   28 non-null     float64
#   2   act_pib  33 non-null     float64
#  
#  |    |   anio |   id_pib |   act_pib |
#  |---:|-------:|---------:|----------:|
#  |  0 |   1990 |      nan |      0.33 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='medida', values_to_col='valor')
#  RangeIndex: 68 entries, 0 to 67
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    68 non-null     int64  
#   1   medida  68 non-null     object 
#   2   valor   61 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  0 |   1990 | id_pib   |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 61 entries, 6 to 66
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    61 non-null     int64  
#   1   medida  61 non-null     object 
#   2   valor   61 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  6 |   1996 | id_pib   |    0.42 |
#  
#  ------------------------------
#  
#  replace_value(col='medida', curr_value='id_pib', new_value='I+D')
#  Index: 61 entries, 6 to 66
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    61 non-null     int64  
#   1   medida  61 non-null     object 
#   2   valor   61 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  6 |   1996 | I+D      |    0.42 |
#  
#  ------------------------------
#  
#  replace_value(col='medida', curr_value='act_pib', new_value='Act. científico tecnológicas')
#  Index: 61 entries, 6 to 66
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    61 non-null     int64  
#   1   medida  61 non-null     object 
#   2   valor   61 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  6 |   1996 | I+D      |    0.42 |
#  
#  ------------------------------
#  