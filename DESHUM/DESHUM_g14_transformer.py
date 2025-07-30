from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rank_col(df: DataFrame, col:str, rank_col:str, ascending: bool):
    df[rank_col] = df[col].rank(ascending=ascending)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar.isin(['NOR', 'ISL', 'SWE', 'AUS', 'USA', 'CHL', 'ARG', 'URY', 'CHN', 'BRA', 'ZZH.LAC', 'COL', 'ZZK.WORLD', 'BOL', 'ZZE.AS', 'IND', 'ZZJ.SSA', 'AFG', 'YEM'])"),
	drop_col(col=['country'], axis=1),
	long_to_wide(index=['geonombreFundar'], columns='tipo_idh', values='ranking_2022'),
	rank_col(col='IDH', rank_col='rank', ascending=True),
	wide_to_long(primary_keys=['geonombreFundar', 'rank'], value_name='valor', var_name='indicador'),
	sort_values(how='ascending', by=['rank', 'indicador']),
	drop_col(col=['rank'], axis=1),
	rename_cols(map={'name_short': 'categoria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  42 non-null     object
#   1   geonombreFundar  42 non-null     object
#   2   tipo_idh         42 non-null     object
#   3   country          42 non-null     object
#   4   ranking_2022     42 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:------------------|:------------------|:-----------|:----------|---------------:|
#  |  0 | ARG               | Argentina         | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar.isin(['NOR', 'ISL', 'SWE', 'AUS', 'USA', 'CHL', 'ARG', 'URY', 'CHN', 'BRA', 'ZZH.LAC', 'COL', 'ZZK.WORLD', 'BOL', 'ZZE.AS', 'IND', 'ZZJ.SSA', 'AFG', 'YEM'])")
#  Index: 20 entries, 0 to 41
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  20 non-null     object
#   1   geonombreFundar  20 non-null     object
#   2   tipo_idh         20 non-null     object
#   3   country          20 non-null     object
#   4   ranking_2022     20 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:------------------|:------------------|:-----------|:----------|---------------:|
#  |  0 | ARG               | Argentina         | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  drop_col(col=['country'], axis=1)
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  20 non-null     object
#   1   geonombreFundar  20 non-null     object
#   2   tipo_idh         20 non-null     object
#   3   ranking_2022     20 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_idh   |   ranking_2022 |
#  |---:|:------------------|:------------------|:-----------|---------------:|
#  |  0 | ARG               | Argentina         | IDH        |             44 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['geonombreFundar'], columns='tipo_idh', values='ranking_2022')
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  10 non-null     object 
#   1   IDH              10 non-null     int64  
#   2   IDH-P            10 non-null     int64  
#   3   rank             10 non-null     float64
#  
#  |    | geonombreFundar   |   IDH |   IDH-P |   rank |
#  |---:|:------------------|------:|--------:|-------:|
#  |  0 | Argentina         |    44 |      27 |      6 |
#  
#  ------------------------------
#  
#  rank_col(col='IDH', rank_col='rank', ascending=True)
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  10 non-null     object 
#   1   IDH              10 non-null     int64  
#   2   IDH-P            10 non-null     int64  
#   3   rank             10 non-null     float64
#  
#  |    | geonombreFundar   |   IDH |   IDH-P |   rank |
#  |---:|:------------------|------:|--------:|-------:|
#  |  0 | Argentina         |    44 |      27 |      6 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geonombreFundar', 'rank'], value_name='valor', var_name='indicador')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  20 non-null     object 
#   1   rank             20 non-null     float64
#   2   indicador        20 non-null     object 
#   3   valor            20 non-null     int64  
#  
#  |    | geonombreFundar   |   rank | indicador   |   valor |
#  |---:|:------------------|-------:|:------------|--------:|
#  |  0 | Argentina         |      6 | IDH         |      44 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['rank', 'indicador'])
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  20 non-null     object 
#   1   rank             20 non-null     float64
#   2   indicador        20 non-null     object 
#   3   valor            20 non-null     int64  
#  
#  |    | geonombreFundar   |   rank | indicador   |   valor |
#  |---:|:------------------|-------:|:------------|--------:|
#  |  0 | Noruega           |      1 | IDH         |       2 |
#  
#  ------------------------------
#  
#  drop_col(col=['rank'], axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geonombreFundar  20 non-null     object
#   1   indicador        20 non-null     object
#   2   valor            20 non-null     int64 
#  
#  |    | geonombreFundar   | indicador   |   valor |
#  |---:|:------------------|:------------|--------:|
#  |  0 | Noruega           | IDH         |       2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'name_short': 'categoria'})
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geonombreFundar  20 non-null     object
#   1   indicador        20 non-null     object
#   2   valor            20 non-null     int64 
#  
#  |    | geonombreFundar   | indicador   |   valor |
#  |---:|:------------------|:------------|--------:|
#  |  0 | Noruega           | IDH         |       2 |
#  
#  ------------------------------
#  