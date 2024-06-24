from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
query(condition="province == 'argentina'"),
	drop_col(col='province', axis=1),
	rename_cols(map={'year': 'anio', 'nbi_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 104 entries, 0 to 103
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   province  104 non-null    object 
#   1   year      104 non-null    int64  
#   2   nbi_rate  104 non-null    float64
#  
#  |    | province   |   year |   nbi_rate |
#  |---:|:-----------|-------:|-----------:|
#  |  0 | argentina  |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  query(condition="province == 'argentina'")
#  Index: 4 entries, 0 to 3
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   province  4 non-null      object 
#   1   year      4 non-null      int64  
#   2   nbi_rate  4 non-null      float64
#  
#  |    | province   |   year |   nbi_rate |
#  |---:|:-----------|-------:|-----------:|
#  |  0 | argentina  |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  drop_col(col='province', axis=1)
#  Index: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      4 non-null      int64  
#   1   nbi_rate  4 non-null      float64
#  
#  |    |   year |   nbi_rate |
#  |---:|-------:|-----------:|
#  |  0 |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'nbi_rate': 'valor'})
#  Index: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    4 non-null      int64  
#   1   valor   4 non-null      float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1980 |    22.3 |
#  
#  ------------------------------
#  