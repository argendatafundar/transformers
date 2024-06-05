from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='orden', axis=1),
	drop_col(col='pais', axis=1),
	rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'})
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
#  drop_col(col='orden', axis=1)
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   pais       109 non-null    object 
#   1   code       109 non-null    object 
#   2   indicador  109 non-null    float64
#  
#  |    | pais   | code   |   indicador |
#  |---:|:-------|:-------|------------:|
#  |  0 | Bhutan | BTN    |        0.21 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   code       109 non-null    object 
#   1   indicador  109 non-null    float64
#  
#  |    | code   |   indicador |
#  |---:|:-------|------------:|
#  |  0 | BTN    |        0.21 |
#  
#  ------------------------------
#  
#  rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'})
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