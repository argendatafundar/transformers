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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='continente_fundar', axis=1),
	drop_col(col='pais_desc', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'tasa_desempleo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20832 entries, 0 to 20831
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               20832 non-null  object 
#   1   pais_desc          20832 non-null  object 
#   2   continente_fundar  20832 non-null  object 
#   3   anio               20832 non-null  int64  
#   4   sexo               20832 non-null  object 
#   5   tasa_desempleo     17949 non-null  float64
#  
#  |    | iso3   | pais_desc   | continente_fundar                      |   anio | sexo   |   tasa_desempleo |
#  |---:|:-------|:------------|:---------------------------------------|-------:|:-------|-----------------:|
#  |  0 | ABW    | Aruba       | América del Norte, Central y el Caribe |   1991 | Total  |              nan |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 20832 entries, 0 to 20831
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            20832 non-null  object 
#   1   pais_desc       20832 non-null  object 
#   2   anio            20832 non-null  int64  
#   3   sexo            20832 non-null  object 
#   4   tasa_desempleo  17949 non-null  float64
#  
#  |    | iso3   | pais_desc   |   anio | sexo   |   tasa_desempleo |
#  |---:|:-------|:------------|-------:|:-------|-----------------:|
#  |  0 | ABW    | Aruba       |   1991 | Total  |              nan |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  RangeIndex: 20832 entries, 0 to 20831
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            20832 non-null  object 
#   1   anio            20832 non-null  int64  
#   2   sexo            20832 non-null  object 
#   3   tasa_desempleo  17949 non-null  float64
#  
#  |    | iso3   |   anio | sexo   |   tasa_desempleo |
#  |---:|:-------|-------:|:-------|-----------------:|
#  |  0 | ABW    |   1991 | Total  |              nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'tasa_desempleo': 'valor'})
#  RangeIndex: 20832 entries, 0 to 20831
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  20832 non-null  object 
#   1   anio       20832 non-null  int64  
#   2   sexo       20832 non-null  object 
#   3   valor      17949 non-null  float64
#  
#  |    | geocodigo   |   anio | sexo   |   valor |
#  |---:|:------------|-------:|:-------|--------:|
#  |  0 | ABW         |   1991 | Total  |     nan |
#  
#  ------------------------------
#  