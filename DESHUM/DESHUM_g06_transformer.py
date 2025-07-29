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
	rename_cols(map={'anios_prom_educ': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6254 entries, 0 to 6253
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6254 non-null   object 
#   1   geonombreFundar    6254 non-null   object 
#   2   continente_fundar  5891 non-null   object 
#   3   es_agregacion      5891 non-null   float64
#   4   anio               6254 non-null   int64  
#   5   anios_prom_educ    6254 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   anios_prom_educ |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |              0.87 |
#  
#  ------------------------------
#  
#  rename_cols(map={'anios_prom_educ': 'valor'})
#  RangeIndex: 6254 entries, 0 to 6253
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6254 non-null   object 
#   1   geonombreFundar    6254 non-null   object 
#   2   continente_fundar  5891 non-null   object 
#   3   es_agregacion      5891 non-null   float64
#   4   anio               6254 non-null   int64  
#   5   valor              6254 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |    0.87 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6254 entries, 0 to 6253
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6254 non-null   object 
#   1   anio             6254 non-null   int64  
#   2   valor            6254 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 |    0.87 |
#  
#  ------------------------------
#  