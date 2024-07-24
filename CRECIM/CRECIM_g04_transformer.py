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
	drop_col(col='medida_bienestar', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'}),
	wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador'),
	query(condition='anio == 2021'),
	drop_col(col='anio', axis=1)
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
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 2059 entries, 0 to 2058
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    2059 non-null   object 
#   1   pais_nombre             2059 non-null   object 
#   2   continente_fundar       2059 non-null   object 
#   3   anio                    2059 non-null   int64  
#   4   pib_percapita_ppp_2017  2059 non-null   float64
#   5   medida_bienestar        2059 non-null   object 
#   6   promedio                2059 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio |
#  |---:|:-------|:--------------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|
#  |  0 | AGO    | Angola        | África              |   2000 |                  1832.51 | consumo            |       7.37 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 2059 entries, 0 to 2058
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    2059 non-null   object 
#   1   continente_fundar       2059 non-null   object 
#   2   anio                    2059 non-null   int64  
#   3   pib_percapita_ppp_2017  2059 non-null   float64
#   4   medida_bienestar        2059 non-null   object 
#   5   promedio                2059 non-null   float64
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio |
#  |---:|:-------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|
#  |  0 | AGO    | África              |   2000 |                  1832.51 | consumo            |       7.37 |
#  
#  ------------------------------
#  
#  drop_col(col='medida_bienestar', axis=1)
#  RangeIndex: 2059 entries, 0 to 2058
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    2059 non-null   object 
#   1   continente_fundar       2059 non-null   object 
#   2   anio                    2059 non-null   int64  
#   3   pib_percapita_ppp_2017  2059 non-null   float64
#   4   promedio                2059 non-null   float64
#  
#  |    | iso3   | continente_fundar   |   anio |   pib_percapita_ppp_2017 |   promedio |
#  |---:|:-------|:--------------------|-------:|-------------------------:|-----------:|
#  |  0 | AGO    | África              |   2000 |                  1832.51 |       7.37 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'})
#  RangeIndex: 2059 entries, 0 to 2058
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigo               2059 non-null   object 
#   1   grupo                   2059 non-null   object 
#   2   anio                    2059 non-null   int64  
#   3   pib_percapita_ppp_2017  2059 non-null   float64
#   4   promedio                2059 non-null   float64
#  
#  |    | geocodigo   | grupo   |   anio |   pib_percapita_ppp_2017 |   promedio |
#  |---:|:------------|:--------|-------:|-------------------------:|-----------:|
#  |  0 | AGO         | África  |   2000 |                  1832.51 |       7.37 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 4118 entries, 0 to 4117
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  4118 non-null   object 
#   1   grupo      4118 non-null   object 
#   2   anio       4118 non-null   int64  
#   3   indicador  4118 non-null   object 
#   4   valor      4118 non-null   float64
#  
#  |    | geocodigo   | grupo   |   anio | indicador              |   valor |
#  |---:|:------------|:--------|-------:|:-----------------------|--------:|
#  |  0 | AGO         | África  |   2000 | pib_percapita_ppp_2017 | 1832.51 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021')
#  Index: 34 entries, 39 to 4007
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  34 non-null     object 
#   1   grupo      34 non-null     object 
#   2   anio       34 non-null     int64  
#   3   indicador  34 non-null     object 
#   4   valor      34 non-null     float64
#  
#  |    | geocodigo   | grupo   |   anio | indicador              |   valor |
#  |---:|:------------|:--------|-------:|:-----------------------|--------:|
#  | 39 | ARM         | Asia    |   2021 | pib_percapita_ppp_2017 | 4522.32 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 34 entries, 39 to 4007
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  34 non-null     object 
#   1   grupo      34 non-null     object 
#   2   indicador  34 non-null     object 
#   3   valor      34 non-null     float64
#  
#  |    | geocodigo   | grupo   | indicador              |   valor |
#  |---:|:------------|:--------|:-----------------------|--------:|
#  | 39 | ARM         | Asia    | pib_percapita_ppp_2017 | 4522.32 |
#  
#  ------------------------------
#  