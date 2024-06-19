from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'}),
	drop_col(col=['orden', 'pais'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   orden      109 non-null    int64  
#   1   pais       109 non-null    object 
#   2   code       109 non-null    object 
#   3   indicador  109 non-null    float64
#  
#  |    |   orden | pais   | code   |   indicador |
#  |---:|--------:|:-------|:-------|------------:|
#  |  0 |     109 | Bhutan | BTN    |        0.21 |
#  
#  ------------------------------
#  
#  rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'})
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   orden      109 non-null    int64  
#   1   pais       109 non-null    object 
#   2   geocodigo  109 non-null    object 
#   3   valor      109 non-null    float64
#  
#  |    |   orden | pais   | geocodigo   |   valor |
#  |---:|--------:|:-------|:------------|--------:|
#  |  0 |     109 | Bhutan | BTN         |    0.21 |
#  
#  ------------------------------
#  
#  drop_col(col=['orden', 'pais'], axis=1)
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  109 non-null    object 
#   1   valor      109 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | BTN         |    0.21 |
#  
#  ------------------------------
#  