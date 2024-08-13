from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def df_merge_geonomenclador(df: DataFrame, geomapper:dict, left:str = 'iso3', right:str = 'geocodigo', how:str='left'):
    import pandas as pd
    geonomenclador_df = pd.DataFrame(geomapper)
    df =  df.merge(geonomenclador_df, left_on = left, right_on = right, how = how)
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
df_merge_geonomenclador(geomapper={'geocodigo': {12: 'ARG', 37: 'BOL', 38: 'BRA', 51: 'CHL', 58: 'COL', 61: 'CRI', 75: 'DOM', 82: 'ECU', 242: 'MEX', 281: 'PAN', 285: 'PER', 295: 'PRY', 320: 'SLV', 369: 'URY'}, 'name_short': {12: 'Argentina', 37: 'Bolivia', 38: 'Brasil', 51: 'Chile', 58: 'Colombia', 61: 'Costa Rica', 75: 'Rep. Dominicana', 82: 'Ecuador', 242: 'México', 281: 'Panamá', 285: 'Perú', 295: 'Paraguay', 320: 'El Salvador', 369: 'Uruguay'}}, left='country_code', right='geocodigo', how='left'),
	drop_col(col=['country_code', 'geocodigo'], axis=1),
	rename_cols(map={'name_short': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'})
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
#  df_merge_geonomenclador(geomapper={'geocodigo': {12: 'ARG', 37: 'BOL', 38: 'BRA', 51: 'CHL', 58: 'COL', 61: 'CRI', 75: 'DOM', 82: 'ECU', 242: 'MEX', 281: 'PAN', 285: 'PER', 295: 'PRY', 320: 'SLV', 369: 'URY'}, 'name_short': {12: 'Argentina', 37: 'Bolivia', 38: 'Brasil', 51: 'Chile', 58: 'Colombia', 61: 'Costa Rica', 75: 'Rep. Dominicana', 82: 'Ecuador', 242: 'México', 281: 'Panamá', 285: 'Perú', 295: 'Paraguay', 320: 'El Salvador', 369: 'Uruguay'}}, left='country_code', right='geocodigo', how='left')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   country_code   14 non-null     object 
#   1   year           14 non-null     int64  
#   2   ipcf_promedio  14 non-null     float64
#   3   geocodigo      14 non-null     object 
#   4   name_short     14 non-null     object 
#  
#  |    | country_code   |   year |   ipcf_promedio | geocodigo   | name_short   |
#  |---:|:---------------|-------:|----------------:|:------------|:-------------|
#  |  0 | ARG            |   2022 |         695.306 | ARG         | Argentina    |
#  
#  ------------------------------
#  
#  drop_col(col=['country_code', 'geocodigo'], axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           14 non-null     int64  
#   1   ipcf_promedio  14 non-null     float64
#   2   name_short     14 non-null     object 
#  
#  |    |   year |   ipcf_promedio | name_short   |
#  |---:|-------:|----------------:|:-------------|
#  |  0 |   2022 |         695.306 | Argentina    |
#  
#  ------------------------------
#  
#  rename_cols(map={'name_short': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       14 non-null     int64  
#   1   valor      14 non-null     float64
#   2   categoria  14 non-null     object 
#  
#  |    |   anio |   valor | categoria   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   2022 | 695.306 | Argentina   |
#  
#  ------------------------------
#  