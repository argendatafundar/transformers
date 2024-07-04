from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='variable', curr_value='EPH', new_value='Datos EPH'),
	replace_value(col='variable', curr_value='ajustados', new_value='Datos ajustados'),
	rename_cols(map={'decil': 'categoria', 'variable': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | EPH        |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='EPH', new_value='Datos EPH')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='ajustados', new_value='Datos ajustados')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  rename_cols(map={'decil': 'categoria', 'variable': 'indicador'})
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  20 non-null     int64  
#   1   indicador  20 non-null     object 
#   2   valor      20 non-null     float64
#  
#  |    |   categoria | indicador   |   valor |
#  |---:|------------:|:------------|--------:|
#  |  0 |           1 | Datos EPH   |   1.904 |
#  
#  ------------------------------
#  