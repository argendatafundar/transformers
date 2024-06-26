from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
df_merge_geonomenclador(left='country_code', right='geocodigo', how='left'),
	drop_col(col=['country_code', 'geocodigo', 'name_short', 'iso_2'], axis=1),
	rename_cols(map={'name_long': 'categoria', 'year': 'indicador', 'ipcf_promedio': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   country_code   14 non-null     object 
#   1   year           14 non-null     int64  
#   2   ipcf_promedio  14 non-null     float64
#  
#  |    | country_code   |   year |   ipcf_promedio |
#  |---:|:---------------|-------:|----------------:|
#  |  0 | ARG            |   2022 |         695.306 |
#  
#  ------------------------------
#  
#  df_merge_geonomenclador(left='country_code', right='geocodigo', how='left')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 7 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   country_code   14 non-null     object 
#   1   year           14 non-null     int64  
#   2   ipcf_promedio  14 non-null     float64
#   3   geocodigo      14 non-null     object 
#   4   name_long      14 non-null     object 
#   5   name_short     14 non-null     object 
#   6   iso_2          14 non-null     object 
#  
#  |    | country_code   |   year |   ipcf_promedio | geocodigo   | name_long   | name_short   | iso_2   |
#  |---:|:---------------|-------:|----------------:|:------------|:------------|:-------------|:--------|
#  |  0 | ARG            |   2022 |         695.306 | ARG         | Argentina   | Argentina    | AR      |
#  
#  ------------------------------
#  
#  drop_col(col=['country_code', 'geocodigo', 'name_short', 'iso_2'], axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           14 non-null     int64  
#   1   ipcf_promedio  14 non-null     float64
#   2   name_long      14 non-null     object 
#  
#  |    |   year |   ipcf_promedio | name_long   |
#  |---:|-------:|----------------:|:------------|
#  |  0 |   2022 |         695.306 | Argentina   |
#  
#  ------------------------------
#  
#  rename_cols(map={'name_long': 'categoria', 'year': 'indicador', 'ipcf_promedio': 'valor'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  14 non-null     int64  
#   1   valor      14 non-null     float64
#   2   categoria  14 non-null     object 
#  
#  |    |   indicador |   valor | categoria   |
#  |---:|------------:|--------:|:------------|
#  |  0 |        2022 | 695.306 | Argentina   |
#  
#  ------------------------------
#  