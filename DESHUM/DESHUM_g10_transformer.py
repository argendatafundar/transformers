from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	rename_cols(map={'idhd': 'valor'}),
	drop_col(col=['continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2106 entries, 0 to 2105
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    2106 non-null   object 
#   1   geonombreFundar    2106 non-null   object 
#   2   continente_fundar  1963 non-null   object 
#   3   es_agregacion      1963 non-null   float64
#   4   anio               2106 non-null   int64  
#   5   idhd               2106 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   idhd |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   2010 |  0.287 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idhd': 'valor'})
#  RangeIndex: 2106 entries, 0 to 2105
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    2106 non-null   object 
#   1   geonombreFundar    2106 non-null   object 
#   2   continente_fundar  1963 non-null   object 
#   3   es_agregacion      1963 non-null   float64
#   4   anio               2106 non-null   int64  
#   5   valor              2106 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   2010 |   0.287 |
#  
#  ------------------------------
#  
#  drop_col(col=['continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 2106 entries, 0 to 2105
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  2106 non-null   object 
#   1   geonombreFundar  2106 non-null   object 
#   2   anio             2106 non-null   int64  
#   3   valor            2106 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   2010 |   0.287 |
#  
#  ------------------------------
#  