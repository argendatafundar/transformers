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
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	rename_columns(prop='valor', provincia='categoria')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 675 entries, 0 to 674
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  675 non-null    object 
#   1   anio       675 non-null    int64  
#   2   prop       675 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 25 entries, 26 to 674
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   anio       25 non-null     int64  
#   2   prop       25 non-null     float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  | 26 | CABA        |   2022 | 0.581325 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 25 entries, 26 to 674
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   prop       25 non-null     float64
#  
#  |    | provincia   |     prop |
#  |---:|:------------|---------:|
#  | 26 | CABA        | 0.581325 |
#  
#  ------------------------------
#  
#  rename_columns(prop='valor', provincia='categoria')
#  Index: 25 entries, 26 to 674
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | categoria   |    valor |
#  |---:|:------------|---------:|
#  | 26 | CABA        | 0.581325 |
#  
#  ------------------------------
#  