from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
query(condition='anio == 2021'),
	drop_col(col='pais_nombre', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='promedio', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'medida_bienestar': 'indicador', 'pib_percapita_ppp_2017': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2059 entries, 0 to 2058
#  Data columns (total 8 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    2059 non-null   object 
#   1   pais_nombre             2059 non-null   object 
#   2   continente_fundar       2059 non-null   object 
#   3   anio                    2059 non-null   int64  
#   4   pib_percapita_ppp_2017  2059 non-null   float64
#   5   medida_bienestar        2059 non-null   object 
#   6   promedio                2059 non-null   float64
#   7   nivel_agregacion        2059 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|:-------------------|
#  |  0 | AGO    | Angola        | África              |   2000 |                  1832.51 | consumo            |       7.37 | pais               |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021')
#  Index: 17 entries, 39 to 1948
#  Data columns (total 8 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    17 non-null     object 
#   1   pais_nombre             17 non-null     object 
#   2   continente_fundar       17 non-null     object 
#   3   anio                    17 non-null     int64  
#   4   pib_percapita_ppp_2017  17 non-null     float64
#   5   medida_bienestar        17 non-null     object 
#   6   promedio                17 non-null     float64
#   7   nivel_agregacion        17 non-null     object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|:-------------------|
#  | 39 | ARM    | Armenia       | Asia                |   2021 |                  4522.32 | consumo            |       7.78 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  Index: 17 entries, 39 to 1948
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    17 non-null     object 
#   1   continente_fundar       17 non-null     object 
#   2   anio                    17 non-null     int64  
#   3   pib_percapita_ppp_2017  17 non-null     float64
#   4   medida_bienestar        17 non-null     object 
#   5   promedio                17 non-null     float64
#   6   nivel_agregacion        17 non-null     object 
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio | nivel_agregacion   |
#  |---:|:-------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|:-------------------|
#  | 39 | ARM    | Asia                |   2021 |                  4522.32 | consumo            |       7.78 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  Index: 17 entries, 39 to 1948
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    17 non-null     object 
#   1   continente_fundar       17 non-null     object 
#   2   anio                    17 non-null     int64  
#   3   pib_percapita_ppp_2017  17 non-null     float64
#   4   medida_bienestar        17 non-null     object 
#   5   promedio                17 non-null     float64
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio |
#  |---:|:-------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|
#  | 39 | ARM    | Asia                |   2021 |                  4522.32 | consumo            |       7.78 |
#  
#  ------------------------------
#  
#  drop_col(col='promedio', axis=1)
#  Index: 17 entries, 39 to 1948
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    17 non-null     object 
#   1   continente_fundar       17 non-null     object 
#   2   anio                    17 non-null     int64  
#   3   pib_percapita_ppp_2017  17 non-null     float64
#   4   medida_bienestar        17 non-null     object 
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |
#  |---:|:-------|:--------------------|-------:|-------------------------:|:-------------------|
#  | 39 | ARM    | Asia                |   2021 |                  4522.32 | consumo            |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'medida_bienestar': 'indicador', 'pib_percapita_ppp_2017': 'valor'})
#  Index: 17 entries, 39 to 1948
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  17 non-null     object 
#   1   grupo      17 non-null     object 
#   2   anio       17 non-null     int64  
#   3   valor      17 non-null     float64
#   4   indicador  17 non-null     object 
#  
#  |    | geocodigo   | grupo   |   anio |   valor | indicador   |
#  |---:|:------------|:--------|-------:|--------:|:------------|
#  | 39 | ARM         | Asia    |   2021 | 4522.32 | consumo     |
#  
#  ------------------------------
#  