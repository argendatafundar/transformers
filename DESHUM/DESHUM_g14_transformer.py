from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def df_merge_geonomenclador(df: DataFrame, left:str = 'iso3', right:str = 'geocodigo', how:str='left'):
    df =  df.merge(geonomenclador, left_on = left, right_on = right, how = how)
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
query(condition="iso3.isin(['NOR', 'ISL', 'SWE', 'AUS', 'USA', 'CHL', 'ARG', 'URY', 'CHN', 'BRA', 'ZZH.LAC', 'COL', 'ZZK.WORLD', 'BOL', 'ZZE.AS', 'IND', 'ZZJ.SSA', 'AFG', 'YEM'])"),
	replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD'),
	replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD'),
	replace_value(col='iso3', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD'),
	replace_value(col='iso3', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD'),
	replace_value(col='iso3', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD'),
	replace_value(col='iso3', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS'),
	replace_value(col='iso3', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP'),
	replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD'),
	replace_value(col='iso3', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA'),
	replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC'),
	replace_value(col='iso3', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA'),
	replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA'),
	df_merge_geonomenclador(left='iso3', right='geocodigo', how='left'),
	drop_col(col=['iso3', 'geocodigo', 'country', 'name_short', 'iso_2'], axis=1),
	rename_cols(map={'tipo_idh': 'indicador', 'name_long': 'categoria', 'ranking_2022': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          42 non-null     object
#   1   tipo_idh      42 non-null     object
#   2   country       42 non-null     object
#   3   ranking_2022  42 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  query(condition="iso3.isin(['NOR', 'ISL', 'SWE', 'AUS', 'USA', 'CHL', 'ARG', 'URY', 'CHN', 'BRA', 'ZZH.LAC', 'COL', 'ZZK.WORLD', 'BOL', 'ZZE.AS', 'IND', 'ZZJ.SSA', 'AFG', 'YEM'])")
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:-------|:-----------|:----------|---------------:|
#  |  0 | ARG    | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  df_merge_geonomenclador(left='iso3', right='geocodigo', how='left')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   iso3          20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#   4   geocodigo     20 non-null     object
#   5   name_long     20 non-null     object
#   6   name_short    20 non-null     object
#   7   iso_2         20 non-null     object
#  
#  |    | iso3   | tipo_idh   | country   |   ranking_2022 | geocodigo   | name_long   | name_short   | iso_2   |
#  |---:|:-------|:-----------|:----------|---------------:|:------------|:------------|:-------------|:--------|
#  |  0 | ARG    | IDH        | Argentina |             44 | ARG         | Argentina   | Argentina    | AR      |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'geocodigo', 'country', 'name_short', 'iso_2'], axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   tipo_idh      20 non-null     object
#   1   ranking_2022  20 non-null     int64 
#   2   name_long     20 non-null     object
#  
#  |    | tipo_idh   |   ranking_2022 | name_long   |
#  |---:|:-----------|---------------:|:------------|
#  |  0 | IDH        |             44 | Argentina   |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_idh': 'indicador', 'name_long': 'categoria', 'ranking_2022': 'valor'})
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   indicador  20 non-null     object
#   1   valor      20 non-null     int64 
#   2   categoria  20 non-null     object
#  
#  |    | indicador   |   valor | categoria   |
#  |---:|:------------|--------:|:------------|
#  |  0 | IDH         |      44 | Argentina   |
#  
#  ------------------------------
#  