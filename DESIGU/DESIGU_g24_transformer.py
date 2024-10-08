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
	replace_value(col='categoria', curr_value='agua', new_value='Agua'),
	replace_value(col='categoria', curr_value='sanidad', new_value='Saneamiento baño'),
	replace_value(col='categoria', curr_value='cloacas', new_value='Cloacas')
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
#  |  0 |  1992 | agua       |   10.07 |
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
#  |  0 |   1992 | agua        |   10.07 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='agua', new_value='Agua')
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
#  |  0 |   1992 | Agua        |   10.07 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='sanidad', new_value='Saneamiento baño')
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
#  |  0 |   1992 | Agua        |   10.07 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='cloacas', new_value='Cloacas')
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
#  |  0 |   1992 | Agua        |   10.07 |
#  
#  ------------------------------
#  