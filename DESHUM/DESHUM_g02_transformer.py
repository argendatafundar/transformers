from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	rename_cols(map={'idh': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion', 'iso3'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6171 entries, 0 to 6170
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6171 non-null   object 
#   1   pais_nombre        6171 non-null   object 
#   2   continente_fundar  5808 non-null   object 
#   3   es_agregacion      5808 non-null   float64
#   4   anio               6171 non-null   int64  
#   5   idh                6171 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 | 0.284 |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 33 entries, 146 to 178
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               33 non-null     object 
#   1   pais_nombre        33 non-null     object 
#   2   continente_fundar  33 non-null     object 
#   3   es_agregacion      33 non-null     float64
#   4   anio               33 non-null     int64  
#   5   idh                33 non-null     float64
#  
#  |     | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |----:|:-------|:--------------|:--------------------|----------------:|-------:|------:|
#  | 146 | ARG    | Argentina     | América del Sur     |               0 |   1990 | 0.724 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh': 'valor'})
#  Index: 33 entries, 146 to 178
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               33 non-null     object 
#   1   pais_nombre        33 non-null     object 
#   2   continente_fundar  33 non-null     object 
#   3   es_agregacion      33 non-null     float64
#   4   anio               33 non-null     int64  
#   5   valor              33 non-null     float64
#  
#  |     | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |----:|:-------|:--------------|:--------------------|----------------:|-------:|--------:|
#  | 146 | ARG    | Argentina     | América del Sur     |               0 |   1990 |   0.724 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion', 'iso3'], axis=1)
#  Index: 33 entries, 146 to 178
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   valor   33 non-null     float64
#  
#  |     |   anio |   valor |
#  |----:|-------:|--------:|
#  | 146 |   1990 |   0.724 |
#  
#  ------------------------------
#  