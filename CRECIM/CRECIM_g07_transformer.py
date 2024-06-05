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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	drop_col(col='continente_fundar', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'participacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1884 entries, 0 to 1883
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               1884 non-null   object 
#   1   pais_nombre        1884 non-null   object 
#   2   continente_fundar  1884 non-null   object 
#   3   anio               1884 non-null   int64  
#   4   participacion      1884 non-null   float64
#   5   nivel_agregacion   1884 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   participacion | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------:|----------------:|:-------------------|
#  |  0 | AFG    | Afganistán    | Asia                |   1950 |       0.0011205 | pais               |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 16 entries, 42 to 57
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               16 non-null     object 
#   1   pais_nombre        16 non-null     object 
#   2   continente_fundar  16 non-null     object 
#   3   anio               16 non-null     int64  
#   4   participacion      16 non-null     float64
#   5   nivel_agregacion   16 non-null     object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   anio |   participacion | nivel_agregacion   |
#  |---:|:-------|:--------------|:--------------------|-------:|----------------:|:-------------------|
#  | 42 | ARG    | Argentina     | América del Sur     |   1820 |       0.0007462 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 16 entries, 42 to 57
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   pais_nombre        16 non-null     object 
#   1   continente_fundar  16 non-null     object 
#   2   anio               16 non-null     int64  
#   3   participacion      16 non-null     float64
#   4   nivel_agregacion   16 non-null     object 
#  
#  |    | pais_nombre   | continente_fundar   |   anio |   participacion | nivel_agregacion   |
#  |---:|:--------------|:--------------------|-------:|----------------:|:-------------------|
#  | 42 | Argentina     | América del Sur     |   1820 |       0.0007462 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  Index: 16 entries, 42 to 57
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   pais_nombre       16 non-null     object 
#   1   anio              16 non-null     int64  
#   2   participacion     16 non-null     float64
#   3   nivel_agregacion  16 non-null     object 
#  
#  |    | pais_nombre   |   anio |   participacion | nivel_agregacion   |
#  |---:|:--------------|-------:|----------------:|:-------------------|
#  | 42 | Argentina     |   1820 |       0.0007462 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  Index: 16 entries, 42 to 57
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   pais_nombre    16 non-null     object 
#   1   anio           16 non-null     int64  
#   2   participacion  16 non-null     float64
#  
#  |    | pais_nombre   |   anio |   participacion |
#  |---:|:--------------|-------:|----------------:|
#  | 42 | Argentina     |   1820 |       0.0007462 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  Index: 16 entries, 42 to 57
#  Data columns (total 2 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           16 non-null     int64  
#   1   participacion  16 non-null     float64
#  
#  |    |   anio |   participacion |
#  |---:|-------:|----------------:|
#  | 42 |   1820 |       0.0007462 |
#  
#  ------------------------------
#  
#  rename_cols(map={'participacion': 'valor'})
#  Index: 16 entries, 42 to 57
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    16 non-null     int64  
#   1   valor   16 non-null     float64
#  
#  |    |   anio |     valor |
#  |---:|-------:|----------:|
#  | 42 |   1820 | 0.0007462 |
#  
#  ------------------------------
#  