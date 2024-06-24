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
#   1   anio                        16 non-null     int64  
#   2   pais                        16 non-null     object 
#   3   pib_per_capita              16 non-null     float64
#   4   tasa_formalidad_productiva  16 non-null     float64
#  
#  |    | iso3   |   anio | pais      |   pib_per_capita |   tasa_formalidad_productiva |
#  |---:|:-------|-------:|:----------|-----------------:|-----------------------------:|
#  |  0 | ARG    |   2021 | Argentina |          21527.2 |                      0.61333 |
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
#   2   pib_per_capita              16 non-null     float64
#   3   tasa_formalidad_productiva  16 non-null     float64
#  
#  |    | iso3   |   anio |   pib_per_capita |   tasa_formalidad_productiva |
#  |---:|:-------|-------:|-----------------:|-----------------------------:|
#  |  0 | ARG    |   2021 |          21527.2 |                      0.61333 |
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
#  |    | iso3   |   anio | indicador      |   valor |
#  |---:|:-------|-------:|:---------------|--------:|
#  |  0 | ARG    |   2021 | pib_per_capita | 21527.2 |
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
#  |    | geocodigo   |   anio | indicador      |   valor |
#  |---:|:------------|-------:|:---------------|--------:|
#  |  0 | ARG         |   2021 | pib_per_capita | 21527.2 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 22 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  22 non-null     object 
#   1   indicador  22 non-null     object 
#   2   valor      22 non-null     float64
#  
#  |    | geocodigo   | indicador      |   valor |
#  |---:|:------------|:---------------|--------:|
#  |  0 | ARG         | pib_per_capita | 21527.2 |
#  
#  ------------------------------
#  