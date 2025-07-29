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
	rename_cols(map={'expectativa_vida': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6798 non-null   object 
#   1   geonombreFundar    6798 non-null   object 
#   2   continente_fundar  6435 non-null   object 
#   3   es_agregacion      6435 non-null   float64
#   4   anio               6798 non-null   int64  
#   5   expectativa_vida   6798 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   expectativa_vida |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |              45.97 |
#  
#  ------------------------------
#  
#  rename_cols(map={'expectativa_vida': 'valor'})
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6798 non-null   object 
#   1   geonombreFundar    6798 non-null   object 
#   2   continente_fundar  6435 non-null   object 
#   3   es_agregacion      6435 non-null   float64
#   4   anio               6798 non-null   int64  
#   5   valor              6798 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |   45.97 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6798 non-null   object 
#   1   anio             6798 non-null   int64  
#   2   valor            6798 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 |   45.97 |
#  
#  ------------------------------
#  