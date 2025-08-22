from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	query(condition="categoria != 'brecha'"),
	replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1'),
	replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 117 entries, 0 to 116
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       117 non-null    int64  
#   1   variable  117 non-null    object 
#   2   valor     117 non-null    float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1986 | quintil1   |     6.7 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 117 entries, 0 to 116
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       117 non-null    int64  
#   1   categoria  117 non-null    object 
#   2   valor      117 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | quintil1    |     6.7 |
#  
#  ------------------------------
#  
#  query(condition="categoria != 'brecha'")
#  Index: 78 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       78 non-null     int64  
#   1   categoria  78 non-null     object 
#   2   valor      78 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | quintil1    |     6.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1')
#  Index: 78 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       78 non-null     int64  
#   1   categoria  78 non-null     object 
#   2   valor      78 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Quintil 1   |     6.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
#  Index: 78 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       78 non-null     int64  
#   1   categoria  78 non-null     object 
#   2   valor      78 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Quintil 1   |     6.7 |
#  
#  ------------------------------
#  