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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1'),
	replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       62 non-null     int64  
#   1   variable  62 non-null     object 
#   2   valor     62 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |  2.0574 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       62 non-null     int64  
#   1   categoria  62 non-null     object 
#   2   valor      62 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | quintil1    |  2.0574 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1')
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       62 non-null     int64  
#   1   categoria  62 non-null     object 
#   2   valor      62 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |  2.0574 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       62 non-null     int64  
#   1   categoria  62 non-null     object 
#   2   valor      62 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |  2.0574 |
#  
#  ------------------------------
#  