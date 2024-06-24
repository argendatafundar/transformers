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
def df_merge(df: DataFrame, df2: DataFrame,left:str, right:str, how:str='left'):
    df =  df.merge(df2, left_on = left, right_on = right, how = how)
    return df

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
def apply_method(df: DataFrame, col:str, method):
    df[col] = df[col].apply(method)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="anio == anio.max() & iso3.isin(['NOR', 'ISL', 'DNK', 'USA', 'HUN', 'BLR', 'CHL', 'ARG', 'BRA', 'ZZH.LAC', 'ZZK.WORLD' ,'COL', 'ZAF', 'SSD', 'ZZJ.SSA'])"),
	replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD'),
	replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC'),
	replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA'),
	df_merge(df2=           geocodigo                     name_long
0                ABW                         Aruba
1                AFE  África Oriental y Meridional
2                AFG                    Afganistán
3                AFW   África Occidental y Central
4                AGO                        Angola
...              ...                           ...
1002  DESHUM_ZZF.EAP       Este de Asia y Pacífico
1003  DESHUM_ZZG.ECA         Europa y Asia Central
1004  DESHUM_ZZH.LAC    América Latina y el Caribe
1005   DESHUM_ZZI.SA               Asia meridional
1006  DESHUM_ZZJ.SSA           África Subsahariana

[1007 rows x 2 columns], left='iso3', right='geocodigo', how='left'),
	rename_cols(map={'name_long': 'categoria'}),
	drop_col(col=['anio', 'country', 'iso3'], axis=1),
	wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador'),
	apply_method(col='indicador', method=<function <lambda> at 0x000001B18FF8AAC0>)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2678 entries, 0 to 2677
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     2678 non-null   object 
#   1   anio     2678 non-null   int64  
#   2   country  2678 non-null   object 
#   3   idh      2639 non-null   float64
#   4   idh_d    2106 non-null   float64
#  
#  |    | iso3   |   anio | country     |   idh |   idh_d |
#  |---:|:-------|-------:|:------------|------:|--------:|
#  |  0 | AFG    |   2010 | Afghanistan | 0.449 |   0.287 |
#  
#  ------------------------------
#  
#  query(condition="anio == anio.max() & iso3.isin(['NOR', 'ISL', 'DNK', 'USA', 'HUN', 'BLR', 'CHL', 'ARG', 'BRA', 'ZZH.LAC', 'ZZK.WORLD' ,'COL', 'ZAF', 'SSD', 'ZZJ.SSA'])")
#  Index: 15 entries, 77 to 2677
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     15 non-null     object 
#   1   anio     15 non-null     int64  
#   2   country  15 non-null     object 
#   3   idh      15 non-null     float64
#   4   idh_d    15 non-null     float64
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d |
#  |---:|:-------|-------:|:----------|------:|--------:|
#  | 77 | ARG    |   2022 | Argentina | 0.849 |   0.747 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  Index: 15 entries, 77 to 2677
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     15 non-null     object 
#   1   anio     15 non-null     int64  
#   2   country  15 non-null     object 
#   3   idh      15 non-null     float64
#   4   idh_d    15 non-null     float64
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d |
#  |---:|:-------|-------:|:----------|------:|--------:|
#  | 77 | ARG    |   2022 | Argentina | 0.849 |   0.747 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  Index: 15 entries, 77 to 2677
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     15 non-null     object 
#   1   anio     15 non-null     int64  
#   2   country  15 non-null     object 
#   3   idh      15 non-null     float64
#   4   idh_d    15 non-null     float64
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d |
#  |---:|:-------|-------:|:----------|------:|--------:|
#  | 77 | ARG    |   2022 | Argentina | 0.849 |   0.747 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  Index: 15 entries, 77 to 2677
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     15 non-null     object 
#   1   anio     15 non-null     int64  
#   2   country  15 non-null     object 
#   3   idh      15 non-null     float64
#   4   idh_d    15 non-null     float64
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d |
#  |---:|:-------|-------:|:----------|------:|--------:|
#  | 77 | ARG    |   2022 | Argentina | 0.849 |   0.747 |
#  
#  ------------------------------
#  
#  df_merge(df2=           geocodigo                     name_long
#  0                ABW                         Aruba
#  1                AFE  África Oriental y Meridional
#  2                AFG                    Afganistán
#  3                AFW   África Occidental y Central
#  4                AGO                        Angola
#  ...              ...                           ...
#  1002  DESHUM_ZZF.EAP       Este de Asia y Pacífico
#  1003  DESHUM_ZZG.ECA         Europa y Asia Central
#  1004  DESHUM_ZZH.LAC    América Latina y el Caribe
#  1005   DESHUM_ZZI.SA               Asia meridional
#  1006  DESHUM_ZZJ.SSA           África Subsahariana
#  
#  [1007 rows x 2 columns], left='iso3', right='geocodigo', how='left')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 7 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       15 non-null     object 
#   1   anio       15 non-null     int64  
#   2   country    15 non-null     object 
#   3   idh        15 non-null     float64
#   4   idh_d      15 non-null     float64
#   5   geocodigo  15 non-null     object 
#   6   name_long  15 non-null     object 
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d | geocodigo   | name_long   |
#  |---:|:-------|-------:|:----------|------:|--------:|:------------|:------------|
#  |  0 | ARG    |   2022 | Argentina | 0.849 |   0.747 | ARG         | Argentina   |
#  
#  ------------------------------
#  
#  rename_cols(map={'name_long': 'categoria'})
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 7 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       15 non-null     object 
#   1   anio       15 non-null     int64  
#   2   country    15 non-null     object 
#   3   idh        15 non-null     float64
#   4   idh_d      15 non-null     float64
#   5   geocodigo  15 non-null     object 
#   6   categoria  15 non-null     object 
#  
#  |    | iso3   |   anio | country   |   idh |   idh_d | geocodigo   | categoria   |
#  |---:|:-------|-------:|:----------|------:|--------:|:------------|:------------|
#  |  0 | ARG    |   2022 | Argentina | 0.849 |   0.747 | ARG         | Argentina   |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'country', 'iso3'], axis=1)
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   idh        15 non-null     float64
#   1   idh_d      15 non-null     float64
#   2   geocodigo  15 non-null     object 
#   3   categoria  15 non-null     object 
#  
#  |    |   idh |   idh_d | geocodigo   | categoria   |
#  |---:|------:|--------:|:------------|:------------|
#  |  0 | 0.849 |   0.747 | ARG         | Argentina   |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador')
#  RangeIndex: 45 entries, 0 to 44
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  45 non-null     object
#   1   indicador  45 non-null     object
#   2   valor      45 non-null     object
#  
#  |    | categoria   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | Argentina   | IDH         |   0.849 |
#  
#  ------------------------------
#  
#  apply_method(col='indicador', method=<function <lambda> at 0x000001B18FF8AAC0>)
#  RangeIndex: 45 entries, 0 to 44
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  45 non-null     object
#   1   indicador  45 non-null     object
#   2   valor      45 non-null     object
#  
#  |    | categoria   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | Argentina   | IDH         |   0.849 |
#  
#  ------------------------------
#  