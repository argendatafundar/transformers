from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'periodo': 'anio', 'sector': 'categoria', 'brecha': 'valor'}),
	sort_values(how='ascending', by=['anio', 'categoria']),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value='Promedio economia', new_value='Promedio economía'),
	query(condition='anio.isin([1996, 2022])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   periodo  54 non-null     int64  
#   1   brecha   54 non-null     float64
#   2   sector   54 non-null     object 
#  
#  |    |   periodo |   brecha | sector            |
#  |---:|----------:|---------:|:------------------|
#  |  0 |      1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  
#  rename_cols(map={'periodo': 'anio', 'sector': 'categoria', 'brecha': 'valor'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   categoria  54 non-null     object 
#  
#  |    |   anio |    valor | categoria         |
#  |---:|-------:|---------:|:------------------|
#  |  0 |   1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   categoria  54 non-null     object 
#  
#  |    |   anio |   valor | categoria         |
#  |---:|-------:|--------:|:------------------|
#  |  0 |   1996 | 23.7825 | Promedio economía |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   categoria  54 non-null     object 
#  
#  |    |   anio |   valor | categoria         |
#  |---:|-------:|--------:|:------------------|
#  |  0 |   1996 | 23.7825 | Promedio economía |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Promedio economia', new_value='Promedio economía')
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   categoria  54 non-null     object 
#  
#  |    |   anio |   valor | categoria         |
#  |---:|-------:|--------:|:------------------|
#  |  0 |   1996 | 23.7825 | Promedio economía |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1996, 2022])')
#  Index: 4 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   valor      4 non-null      float64
#   2   categoria  4 non-null      object 
#  
#  |    |   anio |   valor | categoria         |
#  |---:|-------:|--------:|:------------------|
#  |  0 |   1996 | 23.7825 | Promedio economía |
#  
#  ------------------------------
#  