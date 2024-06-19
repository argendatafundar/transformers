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
	replace_value(col='categoria', curr_value='giniila', new_value='Gini del ingreso laboral'),
	replace_value(col='categoria', curr_value='giniipcf', new_value='Gini del ingreso per cápita')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       64 non-null     int64  
#   1   variable  64 non-null     object 
#   2   valor     64 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | giniipcf   |   44.78 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | giniipcf    |   44.78 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='giniila', new_value='Gini del ingreso laboral')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | giniipcf    |   44.78 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='giniipcf', new_value='Gini del ingreso per cápita')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria                   |   valor |
#  |---:|-------:|:----------------------------|--------:|
#  |  0 |   1992 | Gini del ingreso per cápita |   44.78 |
#  
#  ------------------------------
#  