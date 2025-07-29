from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	rename_cols(map={'expectativa_educ': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6550 non-null   object 
#   1   geonombreFundar    6550 non-null   object 
#   2   continente_fundar  6187 non-null   object 
#   3   es_agregacion      6187 non-null   float64
#   4   anio               6550 non-null   int64  
#   5   expectativa_educ   6550 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   expectativa_educ |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |               2.94 |
#  
#  ------------------------------
#  
#  rename_cols(map={'expectativa_educ': 'valor'})
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6550 non-null   object 
#   1   geonombreFundar    6550 non-null   object 
#   2   continente_fundar  6187 non-null   object 
#   3   es_agregacion      6187 non-null   float64
#   4   anio               6550 non-null   int64  
#   5   valor              6550 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |    2.94 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6550 non-null   object 
#   1   anio             6550 non-null   int64  
#   2   valor            6550 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 |    2.94 |
#  
#  ------------------------------
#  