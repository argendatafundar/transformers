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

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	rename_cols(map={'prop': 'valor', 'provincia': 'categoria'}),
	multiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['valor']),
	replace_value(col='categoria', curr_value='GBA', new_value='Partidos del GBA')
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
#  rename_cols(map={'prop': 'valor', 'provincia': 'categoria'})
#  Index: 25 entries, 26 to 674
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  | 26 | CABA        | 58.1325 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 25 entries, 26 to 674
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  | 26 | CABA        | 58.1325 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['valor'])
#  RangeIndex: 25 entries, 0 to 24
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Formosa     | 0.10317 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GBA', new_value='Partidos del GBA')
#  RangeIndex: 25 entries, 0 to 24
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Formosa     | 0.10317 |
#  
#  ------------------------------
#  