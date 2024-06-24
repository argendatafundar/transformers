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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	drop_col(col='pais_desc', axis=1),
	rename_cols(map={'tasa_desempleo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            64 non-null     object 
#   1   pais_desc       64 non-null     object 
#   2   anio            64 non-null     int64  
#   3   tasa_desempleo  64 non-null     float64
#  
#  |    | iso3   | pais_desc   |   anio |   tasa_desempleo |
#  |---:|:-------|:------------|-------:|-----------------:|
#  |  0 | ARG    | Argentina   |   1991 |           0.0544 |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            32 non-null     object 
#   1   pais_desc       32 non-null     object 
#   2   anio            32 non-null     int64  
#   3   tasa_desempleo  32 non-null     float64
#  
#  |    | iso3   | pais_desc   |   anio |   tasa_desempleo |
#  |---:|:-------|:------------|-------:|-----------------:|
#  |  0 | ARG    | Argentina   |   1991 |           0.0544 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   pais_desc       32 non-null     object 
#   1   anio            32 non-null     int64  
#   2   tasa_desempleo  32 non-null     float64
#  
#  |    | pais_desc   |   anio |   tasa_desempleo |
#  |---:|:------------|-------:|-----------------:|
#  |  0 | Argentina   |   1991 |           0.0544 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  Index: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            32 non-null     int64  
#   1   tasa_desempleo  32 non-null     float64
#  
#  |    |   anio |   tasa_desempleo |
#  |---:|-------:|-----------------:|
#  |  0 |   1991 |           0.0544 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tasa_desempleo': 'valor'})
#  Index: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    32 non-null     int64  
#   1   valor   32 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1991 |  0.0544 |
#  
#  ------------------------------
#  