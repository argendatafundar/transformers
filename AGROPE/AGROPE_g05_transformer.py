from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3c', axis=1),
	query(condition='anio == 2021'),
	drop_col(col='anio', axis=1),
	rename_cols(map={'pais': 'categoria', 'va_agro_sobre_pbi': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              10 non-null     object 
#   1   pais               10 non-null     object 
#   2   anio               10 non-null     int64  
#   3   va_agro_sobre_pbi  10 non-null     float64
#  
#  |    | iso3c   | pais            |   anio |   va_agro_sobre_pbi |
#  |---:|:--------|:----------------|-------:|--------------------:|
#  |  0 | MIC     | Ingreso mediano |   2021 |             8.89095 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3c', axis=1)
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   pais               10 non-null     object 
#   1   anio               10 non-null     int64  
#   2   va_agro_sobre_pbi  10 non-null     float64
#  
#  |    | pais            |   anio |   va_agro_sobre_pbi |
#  |---:|:----------------|-------:|--------------------:|
#  |  0 | Ingreso mediano |   2021 |             8.89095 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021')
#  Index: 10 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   pais               10 non-null     object 
#   1   anio               10 non-null     int64  
#   2   va_agro_sobre_pbi  10 non-null     float64
#  
#  |    | pais            |   anio |   va_agro_sobre_pbi |
#  |---:|:----------------|-------:|--------------------:|
#  |  0 | Ingreso mediano |   2021 |             8.89095 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 10 entries, 0 to 9
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   pais               10 non-null     object 
#   1   va_agro_sobre_pbi  10 non-null     float64
#  
#  |    | pais            |   va_agro_sobre_pbi |
#  |---:|:----------------|--------------------:|
#  |  0 | Ingreso mediano |             8.89095 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pais': 'categoria', 'va_agro_sobre_pbi': 'valor'})
#  Index: 10 entries, 0 to 9
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  10 non-null     object 
#   1   valor      10 non-null     float64
#  
#  |    | categoria       |   valor |
#  |---:|:----------------|--------:|
#  |  0 | Ingreso mediano | 8.89095 |
#  
#  ------------------------------
#  