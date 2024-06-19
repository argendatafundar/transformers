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

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1'),
	replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5'),
	replace_value(col='categoria', curr_value='media', new_value='Media')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       90 non-null     int64  
#   1   variable  90 non-null     object 
#   2   valor     90 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |  66.123 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       90 non-null     int64  
#   1   categoria  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | quintil1    |  66.123 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1')
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       90 non-null     int64  
#   1   categoria  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |  66.123 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       90 non-null     int64  
#   1   categoria  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |  66.123 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='media', new_value='Media')
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       90 non-null     int64  
#   1   categoria  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |  66.123 |
#  
#  ------------------------------
#  