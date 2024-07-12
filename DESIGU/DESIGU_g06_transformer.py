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
rename_cols(map={'ano': 'anio', 'variable': 'geocodigo'}),
	replace_value(col='geocodigo', curr_value='argentina ', new_value='Argentina'),
	replace_value(col='geocodigo', curr_value='americalatina', new_value='América Latina')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   variable  16 non-null     object 
#   1   ano       16 non-null     int64  
#   2   valor     16 non-null     float64
#  
#  |    | variable      |   ano |   valor |
#  |---:|:--------------|------:|--------:|
#  |  0 | americalatina |  1980 |   48.92 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'geocodigo'})
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16 non-null     object 
#   1   anio       16 non-null     int64  
#   2   valor      16 non-null     float64
#  
#  |    | geocodigo     |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | americalatina |   1980 |   48.92 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='argentina ', new_value='Argentina')
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16 non-null     object 
#   1   anio       16 non-null     int64  
#   2   valor      16 non-null     float64
#  
#  |    | geocodigo     |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | americalatina |   1980 |   48.92 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='americalatina', new_value='América Latina')
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16 non-null     object 
#   1   anio       16 non-null     int64  
#   2   valor      16 non-null     float64
#  
#  |    | geocodigo      |   anio |   valor |
#  |---:|:---------------|-------:|--------:|
#  |  0 | América Latina |   1980 |   48.92 |
#  
#  ------------------------------
#  