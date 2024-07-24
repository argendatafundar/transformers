from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'}),
	wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador'),
	query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5306 entries, 0 to 5305
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    5306 non-null   object 
#   1   pais_nombre             5306 non-null   object 
#   2   continente_fundar       5306 non-null   object 
#   3   anio                    5306 non-null   int64  
#   4   pib_percapita_ppp_2017  5306 non-null   float64
#   5   anios_escol_prom        5306 non-null   float64
#   6   nivel_agregacion        5306 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 |   anios_escol_prom | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------:|-------------------------:|-------------------:|:-------------------|
#  |  0 | AFG    | Afganistán    | Asia                |   2002 |                  1280.46 |               1.52 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 5306 entries, 0 to 5305
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    5306 non-null   object 
#   1   pais_nombre             5306 non-null   object 
#   2   continente_fundar       5306 non-null   object 
#   3   anio                    5306 non-null   int64  
#   4   pib_percapita_ppp_2017  5306 non-null   float64
#   5   anios_escol_prom        5306 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 |   anios_escol_prom |
#  |---:|:-------|:--------------|:--------------------|-------:|-------------------------:|-------------------:|
#  |  0 | AFG    | Afganistán    | Asia                |   2002 |                  1280.46 |               1.52 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 5306 entries, 0 to 5305
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    5306 non-null   object 
#   1   continente_fundar       5306 non-null   object 
#   2   anio                    5306 non-null   int64  
#   3   pib_percapita_ppp_2017  5306 non-null   float64
#   4   anios_escol_prom        5306 non-null   float64
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 |   anios_escol_prom |
#  |---:|:-------|:--------------------|-------:|-------------------------:|-------------------:|
#  |  0 | AFG    | Asia                |   2002 |                  1280.46 |               1.52 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'})
#  RangeIndex: 5306 entries, 0 to 5305
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigo               5306 non-null   object 
#   1   grupo                   5306 non-null   object 
#   2   anio                    5306 non-null   int64  
#   3   pib_percapita_ppp_2017  5306 non-null   float64
#   4   anios_escol_prom        5306 non-null   float64
#  
#  |    | geocodigo   | grupo   |   anio |   pib_percapita_ppp_2017 |   anios_escol_prom |
#  |---:|:------------|:--------|-------:|-------------------------:|-------------------:|
#  |  0 | AFG         | Asia    |   2002 |                  1280.46 |               1.52 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 10612 entries, 0 to 10611
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10612 non-null  object 
#   1   grupo      10612 non-null  object 
#   2   anio       10612 non-null  int64  
#   3   indicador  10612 non-null  object 
#   4   valor      10612 non-null  float64
#  
#  |    | geocodigo   | grupo   |   anio | indicador              |   valor |
#  |---:|:------------|:--------|-------:|:-----------------------|--------:|
#  |  0 | AFG         | Asia    |   2002 | pib_percapita_ppp_2017 | 1280.46 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 364 entries, 19 to 10611
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  364 non-null    object 
#   1   grupo      364 non-null    object 
#   2   anio       364 non-null    int64  
#   3   indicador  364 non-null    object 
#   4   valor      364 non-null    float64
#  
#  |    | geocodigo   | grupo   |   anio | indicador              |   valor |
#  |---:|:------------|:--------|-------:|:-----------------------|--------:|
#  | 19 | AFG         | Asia    |   2021 | pib_percapita_ppp_2017 | 1517.02 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 364 entries, 19 to 10611
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  364 non-null    object 
#   1   grupo      364 non-null    object 
#   2   indicador  364 non-null    object 
#   3   valor      364 non-null    float64
#  
#  |    | geocodigo   | grupo   | indicador              |   valor |
#  |---:|:------------|:--------|:-----------------------|--------:|
#  | 19 | AFG         | Asia    | pib_percapita_ppp_2017 | 1517.02 |
#  
#  ------------------------------
#  