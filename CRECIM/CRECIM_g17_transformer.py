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
	drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3012 entries, 0 to 3011
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               3012 non-null   object 
#   1   pais_nombre        3012 non-null   object 
#   2   continente_fundar  2436 non-null   object 
#   3   anio               3012 non-null   int64  
#   4   cambio_relativo    3012 non-null   float64
#   5   nivel_agregacion   3012 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   cambio_relativo | nivel_agregacion   |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|------------------:|:-------------------|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   2011 |                 0 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 3012 entries, 0 to 3011
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              3012 non-null   object 
#   1   pais_nombre       3012 non-null   object 
#   2   anio              3012 non-null   int64  
#   3   cambio_relativo   3012 non-null   float64
#   4   nivel_agregacion  3012 non-null   object 
#  
#  |    | iso3   | pais_nombre   |   anio |   cambio_relativo | nivel_agregacion   |
#  |---:|:-------|:--------------|-------:|------------------:|:-------------------|
#  |  0 | ABW    | Aruba         |   2011 |                 0 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 3012 entries, 0 to 3011
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             3012 non-null   object 
#   1   pais_nombre      3012 non-null   object 
#   2   anio             3012 non-null   int64  
#   3   cambio_relativo  3012 non-null   float64
#  
#  |    | iso3   | pais_nombre   |   anio |   cambio_relativo |
#  |---:|:-------|:--------------|-------:|------------------:|
#  |  0 | ABW    | Aruba         |   2011 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 3012 entries, 0 to 3011
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             3012 non-null   object 
#   1   anio             3012 non-null   int64  
#   2   cambio_relativo  3012 non-null   float64
#  
#  |    | iso3   |   anio |   cambio_relativo |
#  |---:|:-------|-------:|------------------:|
#  |  0 | ABW    |   2011 |                 0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 3012 entries, 0 to 3011
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3012 non-null   object 
#   1   anio       3012 non-null   int64  
#   2   valor      3012 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   2011 |       0 |
#  
#  ------------------------------
#  