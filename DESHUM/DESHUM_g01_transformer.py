from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	latest_year(by='anio'),
	rename_cols(map={'idh': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6171 entries, 0 to 6170
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6171 non-null   object 
#   1   geonombreFundar    6171 non-null   object 
#   2   continente_fundar  5808 non-null   object 
#   3   es_agregacion      5808 non-null   float64
#   4   anio               6171 non-null   int64  
#   5   idh                6171 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 | 0.284 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    204 non-null    object 
#   1   geonombreFundar    204 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   idh                204 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   idh |
#  |---:|:------------------|:------------------|:--------------------|----------------:|------:|
#  | 32 | AFG               | Afganistán        | Asia                |               0 | 0.462 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh': 'valor'})
#  Index: 204 entries, 32 to 6170
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    204 non-null    object 
#   1   geonombreFundar    204 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   valor              204 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|--------:|
#  | 32 | AFG               | Afganistán        | Asia                |               0 |   0.462 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  204 non-null    object 
#   1   valor            204 non-null    float64
#  
#  |    | geonombreFundar   |   valor |
#  |---:|:------------------|--------:|
#  | 32 | Afganistán        |   0.462 |
#  
#  ------------------------------
#  