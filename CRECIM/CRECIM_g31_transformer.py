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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='continente_fundar', axis=1),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'relativo_arg': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15956 entries, 0 to 15955
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               15956 non-null  object 
#   1   pais_nombre        15956 non-null  object 
#   2   continente_fundar  15746 non-null  object 
#   3   es_agregacion      15956 non-null  int64  
#   4   anio               15956 non-null  int64  
#   5   relativo_arg       15956 non-null  float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   relativo_arg |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1950 |         6.8763 |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 15956 entries, 0 to 15955
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           15956 non-null  object 
#   1   pais_nombre    15956 non-null  object 
#   2   es_agregacion  15956 non-null  int64  
#   3   anio           15956 non-null  int64  
#   4   relativo_arg   15956 non-null  float64
#  
#  |    | iso3   | pais_nombre   |   es_agregacion |   anio |   relativo_arg |
#  |---:|:-------|:--------------|----------------:|-------:|---------------:|
#  |  0 | AFG    | Afganistán    |               0 |   1950 |         6.8763 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  RangeIndex: 15956 entries, 0 to 15955
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          15956 non-null  object 
#   1   pais_nombre   15956 non-null  object 
#   2   anio          15956 non-null  int64  
#   3   relativo_arg  15956 non-null  float64
#  
#  |    | iso3   | pais_nombre   |   anio |   relativo_arg |
#  |---:|:-------|:--------------|-------:|---------------:|
#  |  0 | AFG    | Afganistán    |   1950 |         6.8763 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 15956 entries, 0 to 15955
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          15956 non-null  object 
#   1   anio          15956 non-null  int64  
#   2   relativo_arg  15956 non-null  float64
#  
#  |    | iso3   |   anio |   relativo_arg |
#  |---:|:-------|-------:|---------------:|
#  |  0 | AFG    |   1950 |         6.8763 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'relativo_arg': 'valor'})
#  RangeIndex: 15956 entries, 0 to 15955
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  15956 non-null  object 
#   1   anio       15956 non-null  int64  
#   2   valor      15956 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |  6.8763 |
#  
#  ------------------------------
#  