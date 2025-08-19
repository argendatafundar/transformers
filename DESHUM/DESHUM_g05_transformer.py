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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'inb_pc': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1),
	query(condition='anio in [1990, 2001, 2011, 2022]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6659 non-null   object 
#   1   geonombreFundar    6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  rename_cols(map={'inb_pc': 'valor'})
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6659 non-null   object 
#   1   geonombreFundar    6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   valor              6659 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 | 3115.67 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6659 non-null   object 
#   1   anio             6659 non-null   int64  
#   2   valor            6659 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 | 3115.67 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1990, 2001, 2011, 2022]')
#  Index: 804 entries, 0 to 6658
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  804 non-null    object 
#   1   anio             804 non-null    int64  
#   2   valor            804 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 | 3115.67 |
#  
#  ------------------------------
#  