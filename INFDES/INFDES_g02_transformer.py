from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais', axis=1),
	wide_to_long(primary_keys=['iso3', 'anio'], value_name='valor', var_name='indicador'),
	rename_cols(map={'iso3': 'geocodigo'}),
	latest_year(by='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   iso3                        16 non-null     object 
#   1   pais                        16 non-null     object 
#   2   anio                        16 non-null     int64  
#   3   tasa_formalidad_productiva  16 non-null     float64
#   4   pib_per_capita_ppp          16 non-null     float64
#  
#  |    | iso3   | pais      |   anio |   tasa_formalidad_productiva |   pib_per_capita_ppp |
#  |---:|:-------|:----------|-------:|-----------------------------:|---------------------:|
#  |  0 | ARG    | Argentina |   2022 |                        0.599 |              27127.4 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 4 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   iso3                        16 non-null     object 
#   1   anio                        16 non-null     int64  
#   2   tasa_formalidad_productiva  16 non-null     float64
#   3   pib_per_capita_ppp          16 non-null     float64
#  
#  |    | iso3   |   anio |   tasa_formalidad_productiva |   pib_per_capita_ppp |
#  |---:|:-------|-------:|-----------------------------:|---------------------:|
#  |  0 | ARG    |   2022 |                        0.599 |              27127.4 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['iso3', 'anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       32 non-null     object 
#   1   anio       32 non-null     int64  
#   2   indicador  32 non-null     object 
#   3   valor      32 non-null     float64
#  
#  |    | iso3   |   anio | indicador                  |   valor |
#  |---:|:-------|-------:|:---------------------------|--------:|
#  |  0 | ARG    |   2022 | tasa_formalidad_productiva |   0.599 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  32 non-null     object 
#   1   anio       32 non-null     int64  
#   2   indicador  32 non-null     object 
#   3   valor      32 non-null     float64
#  
#  |    | geocodigo   |   anio | indicador                  |   valor |
#  |---:|:------------|-------:|:---------------------------|--------:|
#  |  0 | ARG         |   2022 | tasa_formalidad_productiva |   0.599 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 20 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  20 non-null     object 
#   1   indicador  20 non-null     object 
#   2   valor      20 non-null     float64
#  
#  |    | geocodigo   | indicador                  |   valor |
#  |---:|:------------|:---------------------------|--------:|
#  |  0 | ARG         | tasa_formalidad_productiva |   0.599 |
#  
#  ------------------------------
#  