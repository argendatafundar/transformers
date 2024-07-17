from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	drop_col(col='pais_nombre', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'anios_escol_prom': 'indicador', 'pib_percapita_ppp_2017': 'valor'})
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
#  latest_year(by='anio')
#  Index: 182 entries, 19 to 5305
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    182 non-null    object 
#   1   pais_nombre             182 non-null    object 
#   2   continente_fundar       182 non-null    object 
#   3   pib_percapita_ppp_2017  182 non-null    float64
#   4   anios_escol_prom        182 non-null    float64
#   5   nivel_agregacion        182 non-null    object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   pib_percapita_ppp_2017 |   anios_escol_prom | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------------------------:|-------------------:|:-------------------|
#  | 19 | AFG    | Afganistán    | Asia                |                  1517.02 |               2.99 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  Index: 182 entries, 19 to 5305
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    182 non-null    object 
#   1   continente_fundar       182 non-null    object 
#   2   pib_percapita_ppp_2017  182 non-null    float64
#   3   anios_escol_prom        182 non-null    float64
#   4   nivel_agregacion        182 non-null    object 
#  
#  |    | iso3   | continente_fundar   |   pib_percapita_ppp_2017 |   anios_escol_prom | nivel_agregacion   |
#  |---:|:-------|:--------------------|-------------------------:|-------------------:|:-------------------|
#  | 19 | AFG    | Asia                |                  1517.02 |               2.99 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  Index: 182 entries, 19 to 5305
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    182 non-null    object 
#   1   continente_fundar       182 non-null    object 
#   2   pib_percapita_ppp_2017  182 non-null    float64
#   3   anios_escol_prom        182 non-null    float64
#  
#  |    | iso3   | continente_fundar   |   pib_percapita_ppp_2017 |   anios_escol_prom |
#  |---:|:-------|:--------------------|-------------------------:|-------------------:|
#  | 19 | AFG    | Asia                |                  1517.02 |               2.99 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'anios_escol_prom': 'indicador', 'pib_percapita_ppp_2017': 'valor'})
#  Index: 182 entries, 19 to 5305
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  182 non-null    object 
#   1   grupo      182 non-null    object 
#   2   valor      182 non-null    float64
#   3   indicador  182 non-null    float64
#  
#  |    | geocodigo   | grupo   |   valor |   indicador |
#  |---:|:------------|:--------|--------:|------------:|
#  | 19 | AFG         | Asia    | 1517.02 |        2.99 |
#  
#  ------------------------------
#  