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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col=['pais_nombre', 'anio', 'medida_bienestar'], axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'medida_bienestar': 'indicador'}),
	wide_to_long(primary_keys=['geocodigo', 'grupo'], value_name='valor', var_name='indicador')
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
#  query(condition='anio == anio.max()')
#  Index: 1 entries, 926 to 926
#  Data columns (total 8 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    1 non-null      object 
#   1   pais_nombre             1 non-null      object 
#   2   continente_fundar       1 non-null      object 
#   3   anio                    1 non-null      int64  
#   4   pib_percapita_ppp_2017  1 non-null      float64
#   5   medida_bienestar        1 non-null      object 
#   6   promedio                1 non-null      float64
#   7   nivel_agregacion        1 non-null      object 
#  
#  |     | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_percapita_ppp_2017 | medida_bienestar   |   promedio | nivel_agregacion   |
#  |----:|:-------|:--------------|:--------------------|-------:|-------------------------:|:-------------------|-----------:|:-------------------|
#  | 926 | IDN    | Indonesia     | Asia                |   2022 |                  4061.17 | consumo            |       7.81 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'anio', 'medida_bienestar'], axis=1)
#  Index: 1 entries, 926 to 926
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    1 non-null      object 
#   1   continente_fundar       1 non-null      object 
#   2   pib_percapita_ppp_2017  1 non-null      float64
#   3   promedio                1 non-null      float64
#   4   nivel_agregacion        1 non-null      object 
#  
#  |     | iso3   | continente_fundar   |   pib_percapita_ppp_2017 |   promedio | nivel_agregacion   |
#  |----:|:-------|:--------------------|-------------------------:|-----------:|:-------------------|
#  | 926 | IDN    | Asia                |                  4061.17 |       7.81 | pais               |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo', 'medida_bienestar': 'indicador'})
#  Index: 1 entries, 926 to 926
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigo               1 non-null      object 
#   1   grupo                   1 non-null      object 
#   2   pib_percapita_ppp_2017  1 non-null      float64
#   3   promedio                1 non-null      float64
#   4   nivel_agregacion        1 non-null      object 
#  
#  |     | geocodigo   | grupo   |   pib_percapita_ppp_2017 |   promedio | nivel_agregacion   |
#  |----:|:------------|:--------|-------------------------:|-----------:|:-------------------|
#  | 926 | IDN         | Asia    |                  4061.17 |       7.81 | pais               |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo', 'grupo'], value_name='valor', var_name='indicador')
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  3 non-null      object
#   1   grupo      3 non-null      object
#   2   indicador  3 non-null      object
#   3   valor      3 non-null      object
#  
#  |    | geocodigo   | grupo   | indicador              |   valor |
#  |---:|:------------|:--------|:-----------------------|--------:|
#  |  0 | IDN         | Asia    | pib_percapita_ppp_2017 | 4061.17 |
#  
#  ------------------------------
#  