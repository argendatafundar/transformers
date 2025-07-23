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
	rename_columns(iso3='geocodigo', expo='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1351 entries, 0 to 1350
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1351 non-null   object 
#   1   geonombreFundar  1351 non-null   object 
#   2   anio             1351 non-null   int64  
#   3   expo             1351 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   expo |
#  |---:|:------------------|:------------------|-------:|-------:|
#  |  0 | NAM               | Namibia           |   2017 |      0 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 161 entries, 3 to 1350
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  161 non-null    object 
#   1   geonombreFundar  161 non-null    object 
#   2   anio             161 non-null    int64  
#   3   expo             161 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   expo |
#  |---:|:------------------|:------------------|-------:|-------:|
#  |  3 | NAM               | Namibia           |   2022 |      0 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 161 entries, 3 to 1350
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  161 non-null    object 
#   1   geonombreFundar  161 non-null    object 
#   2   expo             161 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   expo |
#  |---:|:------------------|:------------------|-------:|
#  |  3 | NAM               | Namibia           |      0 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', expo='valor')
#  Index: 161 entries, 3 to 1350
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  161 non-null    object 
#   1   geonombreFundar  161 non-null    object 
#   2   valor            161 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  3 | NAM               | Namibia           |       0 |
#  
#  ------------------------------
#  