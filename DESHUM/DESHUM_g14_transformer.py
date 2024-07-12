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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def join_geonomencladores(df: DataFrame, on:str, how: str):
    df = df.merge(geonomenclador, on=on, how=how)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def long_to_wide(df: DataFrame, index: list, columns: str, values: str):
    return df.pivot_table(index=index, columns=columns, values=values).reset_index()

@transformer.convert
def rank_col(df: DataFrame, col:str, rank_col:str, ascending: bool):
    df[rank_col] = df[col].rank(ascending=ascending)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
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
	rename_cols(map={'iso3': 'geocodigo'}),
	join_geonomencladores(on='geocodigo', how='left'),
	drop_col(col=['geocodigo', 'country', 'name_long', 'iso_2'], axis=1),
	long_to_wide(index=['name_short'], columns='tipo_idh', values='ranking_2022'),
	rank_col(col='IDH', rank_col='rank', ascending=False),
	wide_to_long(primary_keys=['name_short', 'rank'], value_name='valor', var_name='indicador'),
	sort_values(how='ascending', by=['rank', 'indicador']),
	drop_col(col=['rank'], axis=1)
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
#  rename_cols(map={'iso3': 'geocodigo'})
#  Index: 20 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   geocodigo     20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#  
#  |    | geocodigo   | tipo_idh   | country   |   ranking_2022 |
#  |---:|:------------|:-----------|:----------|---------------:|
#  |  0 | ARG         | IDH        | Argentina |             44 |
#  
#  ------------------------------
#  
#  join_geonomencladores(on='geocodigo', how='left')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 7 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   geocodigo     20 non-null     object
#   1   tipo_idh      20 non-null     object
#   2   country       20 non-null     object
#   3   ranking_2022  20 non-null     int64 
#   4   name_long     20 non-null     object
#   5   name_short    20 non-null     object
#   6   iso_2         20 non-null     object
#  
#  |    | geocodigo   | tipo_idh   | country   |   ranking_2022 | name_long   | name_short   | iso_2   |
#  |---:|:------------|:-----------|:----------|---------------:|:------------|:-------------|:--------|
#  |  0 | ARG         | IDH        | Argentina |             44 | Argentina   | Argentina    | AR      |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigo', 'country', 'name_long', 'iso_2'], axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype 
#  ---  ------        --------------  ----- 
#   0   tipo_idh      20 non-null     object
#   1   ranking_2022  20 non-null     int64 
#   2   name_short    20 non-null     object
#  
#  |    | tipo_idh   |   ranking_2022 | name_short   |
#  |---:|:-----------|---------------:|:-------------|
#  |  0 | IDH        |             44 | Argentina    |
#  
#  ------------------------------
#  
#  long_to_wide(index=['name_short'], columns='tipo_idh', values='ranking_2022')
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   name_short  10 non-null     object 
#   1   IDH         10 non-null     float64
#   2   IDH-P       10 non-null     float64
#   3   rank        10 non-null     float64
#  
#  |    | name_short   |   IDH |   IDH-P |   rank |
#  |---:|:-------------|------:|--------:|-------:|
#  |  0 | Argentina    |    44 |      27 |      5 |
#  
#  ------------------------------
#  
#  rank_col(col='IDH', rank_col='rank', ascending=False)
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   name_short  10 non-null     object 
#   1   IDH         10 non-null     float64
#   2   IDH-P       10 non-null     float64
#   3   rank        10 non-null     float64
#  
#  |    | name_short   |   IDH |   IDH-P |   rank |
#  |---:|:-------------|------:|--------:|-------:|
#  |  0 | Argentina    |    44 |      27 |      5 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['name_short', 'rank'], value_name='valor', var_name='indicador')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   name_short  20 non-null     object 
#   1   rank        20 non-null     float64
#   2   indicador   20 non-null     object 
#   3   valor       20 non-null     float64
#  
#  |    | name_short   |   rank | indicador   |   valor |
#  |---:|:-------------|-------:|:------------|--------:|
#  |  0 | Argentina    |      5 | IDH         |      44 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['rank', 'indicador'])
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   name_short  20 non-null     object 
#   1   rank        20 non-null     float64
#   2   indicador   20 non-null     object 
#   3   valor       20 non-null     float64
#  
#  |    | name_short   |   rank | indicador   |   valor |
#  |---:|:-------------|-------:|:------------|--------:|
#  |  0 | Colombia     |      1 | IDH         |      75 |
#  
#  ------------------------------
#  
#  drop_col(col=['rank'], axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   name_short  20 non-null     object 
#   1   indicador   20 non-null     object 
#   2   valor       20 non-null     float64
#  
#  |    | name_short   | indicador   |   valor |
#  |---:|:-------------|:------------|--------:|
#  |  0 | Colombia     | IDH         |      75 |
#  
#  ------------------------------
#  