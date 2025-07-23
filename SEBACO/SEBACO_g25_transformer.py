from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	rename_columns(iso3='geocodigo', impo='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1266 entries, 0 to 1265
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1266 non-null   object 
#   1   geonombreFundar  1266 non-null   object 
#   2   anio             1266 non-null   int64  
#   3   impo             1266 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   impo |
#  |---:|:------------------|:------------------|-------:|-------:|
#  |  0 | NAM               | Namibia           |   2015 |      0 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 162 entries, 8 to 1265
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   anio             162 non-null    int64  
#   3   impo             162 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   impo |
#  |---:|:------------------|:------------------|-------:|-------:|
#  |  8 | AND               | Andorra           |   2022 |      0 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 162 entries, 8 to 1265
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   impo             162 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   impo |
#  |---:|:------------------|:------------------|-------:|
#  |  8 | AND               | Andorra           |      0 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', impo='valor')
#  Index: 162 entries, 8 to 1265
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   valor            162 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  8 | AND               | Andorra           |       0 |
#  
#  ------------------------------
#  