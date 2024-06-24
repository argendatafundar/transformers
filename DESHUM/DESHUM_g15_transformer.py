from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	rename_cols(map={'tipo_idh': 'categoria'}),
	drop_col(col=['iso3', 'country'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13596 entries, 0 to 13595
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   iso3      13596 non-null  object 
#   1   anio      13596 non-null  int64  
#   2   tipo_idh  13596 non-null  object 
#   3   country   13596 non-null  object 
#   4   valor     11271 non-null  float64
#  
#  |    | iso3   |   anio | tipo_idh   | country     |   valor |
#  |---:|:-------|-------:|:-----------|:------------|--------:|
#  |  0 | AFG    |   1990 | IDH        | Afghanistan |   0.284 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 66 entries, 330 to 395
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   iso3      66 non-null     object 
#   1   anio      66 non-null     int64  
#   2   tipo_idh  66 non-null     object 
#   3   country   66 non-null     object 
#   4   valor     66 non-null     float64
#  
#  |     | iso3   |   anio | tipo_idh   | country   |   valor |
#  |----:|:-------|-------:|:-----------|:----------|--------:|
#  | 330 | ARG    |   1990 | IDH        | Argentina |   0.724 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_idh': 'categoria'})
#  Index: 66 entries, 330 to 395
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       66 non-null     object 
#   1   anio       66 non-null     int64  
#   2   categoria  66 non-null     object 
#   3   country    66 non-null     object 
#   4   valor      66 non-null     float64
#  
#  |     | iso3   |   anio | categoria   | country   |   valor |
#  |----:|:-------|-------:|:------------|:----------|--------:|
#  | 330 | ARG    |   1990 | IDH         | Argentina |   0.724 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'country'], axis=1)
#  Index: 66 entries, 330 to 395
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 330 |   1990 | IDH         |   0.724 |
#  
#  ------------------------------
#  