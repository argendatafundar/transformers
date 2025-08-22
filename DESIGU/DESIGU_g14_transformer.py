from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='e612', new_value='6 - 12'),
	replace_value(col='categoria', curr_value='e1319', new_value='13 - 19'),
	replace_value(col='categoria', curr_value='e2025', new_value='20 - 25')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       99 non-null     int64  
#   1   variable  99 non-null     object 
#   2   valor     99 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | e612       |   0.997 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       99 non-null     int64  
#   1   categoria  99 non-null     object 
#   2   valor      99 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | e612        |   0.997 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='e612', new_value='6 - 12')
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       99 non-null     int64  
#   1   categoria  99 non-null     object 
#   2   valor      99 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | 6 - 12      |   0.997 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='e1319', new_value='13 - 19')
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       99 non-null     int64  
#   1   categoria  99 non-null     object 
#   2   valor      99 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | 6 - 12      |   0.997 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='e2025', new_value='20 - 25')
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       99 non-null     int64  
#   1   categoria  99 non-null     object 
#   2   valor      99 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | 6 - 12      |   0.997 |
#  
#  ------------------------------
#  