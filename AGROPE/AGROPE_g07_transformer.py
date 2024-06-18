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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'cultivo   ': 'categoria', 'share': 'valor'}),
	drop_col(col='q_total', axis=1),
	drop_col(col='campania', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   cultivo   21 non-null     object 
#   1   campania  21 non-null     object 
#   2   q_total   21 non-null     int64  
#   3   share     21 non-null     float64
#  
#  |    | cultivo   | campania   |   q_total |   share |
#  |---:|:----------|:-----------|----------:|--------:|
#  |  0 | Algodón   | 2021/22    |   1115510 | 0.77728 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cultivo   ': 'categoria', 'share': 'valor'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   cultivo   21 non-null     object 
#   1   campania  21 non-null     object 
#   2   q_total   21 non-null     int64  
#   3   valor     21 non-null     float64
#  
#  |    | cultivo   | campania   |   q_total |   valor |
#  |---:|:----------|:-----------|----------:|--------:|
#  |  0 | Algodón   | 2021/22    |   1115510 | 0.77728 |
#  
#  ------------------------------
#  
#  drop_col(col='q_total', axis=1)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   cultivo   21 non-null     object 
#   1   campania  21 non-null     object 
#   2   valor     21 non-null     float64
#  
#  |    | cultivo   | campania   |   valor |
#  |---:|:----------|:-----------|--------:|
#  |  0 | Algodón   | 2021/22    | 0.77728 |
#  
#  ------------------------------
#  
#  drop_col(col='campania', axis=1)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   cultivo  21 non-null     object 
#   1   valor    21 non-null     float64
#  
#  |    | cultivo   |   valor |
#  |---:|:----------|--------:|
#  |  0 | Algodón   | 0.77728 |
#  
#  ------------------------------
#  