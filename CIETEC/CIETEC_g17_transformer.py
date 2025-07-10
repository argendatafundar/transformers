from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='medida', values_to_col='valor'),
	drop_na(col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     33 non-null     int64  
#   1   i_d_pib  27 non-null     float64
#   2   act_pib  33 non-null     float64
#  
#  |    |   anio |   i_d_pib |   act_pib |
#  |---:|-------:|----------:|----------:|
#  |  0 |   1990 |       nan |      0.33 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='medida', values_to_col='valor')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    66 non-null     int64  
#   1   medida  66 non-null     object 
#   2   valor   60 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  0 |   1990 | i_d_pib  |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 60 entries, 6 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    60 non-null     int64  
#   1   medida  60 non-null     object 
#   2   valor   60 non-null     float64
#  
#  |    |   anio | medida   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  6 |   1996 | i_d_pib  |    0.42 |
#  
#  ------------------------------
#  