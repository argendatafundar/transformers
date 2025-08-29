from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([2007, 2014, 2022])'),
	rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'}),
	sort_values(how='ascending', by=['anio', 'categoria']),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value='Promedio economia', new_value='Promedio economía', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          54 non-null     int64  
#   1   sector        54 non-null     object 
#   2   prop_mujeres  54 non-null     float64
#  
#  |    |   anio | sector            |   prop_mujeres |
#  |---:|-------:|:------------------|---------------:|
#  |  0 |   1996 | Promedio economia |       0.267976 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([2007, 2014, 2022])')
#  Index: 6 entries, 11 to 53
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          6 non-null      int64  
#   1   sector        6 non-null      object 
#   2   prop_mujeres  6 non-null      float64
#  
#  |    |   anio | sector            |   prop_mujeres |
#  |---:|-------:|:------------------|---------------:|
#  | 11 |   2007 | Promedio economia |       0.301087 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'})
#  Index: 6 entries, 11 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6 non-null      int64  
#   1   categoria  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |    |   anio | categoria         |    valor |
#  |---:|-------:|:------------------|---------:|
#  | 11 |   2007 | Promedio economia | 0.301087 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6 non-null      int64  
#   1   categoria  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |    |   anio | categoria         |   valor |
#  |---:|-------:|:------------------|--------:|
#  |  0 |   2007 | Promedio economia | 30.1087 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6 non-null      int64  
#   1   categoria  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |    |   anio | categoria         |   valor |
#  |---:|-------:|:------------------|--------:|
#  |  0 |   2007 | Promedio economia | 30.1087 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Promedio economia', new_value='Promedio economía', mapping=None)
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6 non-null      int64  
#   1   categoria  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |    |   anio | categoria         |   valor |
#  |---:|-------:|:------------------|--------:|
#  |  0 |   2007 | Promedio economía | 30.1087 |
#  
#  ------------------------------
#  