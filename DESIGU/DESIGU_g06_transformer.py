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
rename_cols(map={'variable': 'geocodigo', 'ano': 'anio'}),
	replace_value(col='geocodigo', curr_value='americalatina', new_value='AML-DESIGU'),
	replace_value(col='geocodigo', curr_value='argentina ', new_value='ARG')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       16 non-null     int64  
#   1   variable  16 non-null     object 
#   2   valor     16 non-null     float64
#  
#  |    |   ano | variable      |   valor |
#  |---:|------:|:--------------|--------:|
#  |  0 |  1980 | americalatina |   48.92 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'geocodigo', 'ano': 'anio'})
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       16 non-null     int64  
#   1   geocodigo  16 non-null     object 
#   2   valor      16 non-null     float64
#  
#  |    |   anio | geocodigo     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1980 | americalatina |   48.92 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='americalatina', new_value='AML-DESIGU')
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       16 non-null     int64  
#   1   geocodigo  16 non-null     object 
#   2   valor      16 non-null     float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1980 | AML-DESIGU  |   48.92 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='argentina ', new_value='ARG')
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       16 non-null     int64  
#   1   geocodigo  16 non-null     object 
#   2   valor      16 non-null     float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1980 | AML-DESIGU  |   48.92 |
#  
#  ------------------------------
#  