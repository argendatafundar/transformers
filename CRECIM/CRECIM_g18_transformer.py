from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['provincia_id', 'geocodigoFundar', 'anio'], axis=1),
	multiplicar_por_escalar(col='participacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 456 entries, 0 to 455
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             456 non-null    int64  
#   1   vab_pb           456 non-null    float64
#   2   region           456 non-null    object 
#   3   participacion    456 non-null    float64
#   4   provincia_id     456 non-null    object 
#   5   geocodigoFundar  456 non-null    object 
#   6   geonombreFundar  456 non-null    object 
#  
#  |    |   anio |   vab_pb | region          |   participacion | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|---------:|:----------------|----------------:|:---------------|:------------------|:------------------|
#  |  0 |   2004 |   131866 | Pampeana y AMBA |        0.319959 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 18 to 455
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   vab_pb           24 non-null     float64
#   2   region           24 non-null     object 
#   3   participacion    24 non-null     float64
#   4   provincia_id     24 non-null     object 
#   5   geocodigoFundar  24 non-null     object 
#   6   geonombreFundar  24 non-null     object 
#  
#  |    |   anio |   vab_pb | region          |   participacion | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|---------:|:----------------|----------------:|:---------------|:------------------|:------------------|
#  | 18 |   2022 |   196497 | Pampeana y AMBA |        0.325324 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  drop_col(col=['provincia_id', 'geocodigoFundar', 'anio'], axis=1)
#  Index: 24 entries, 18 to 455
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   vab_pb           24 non-null     float64
#   1   region           24 non-null     object 
#   2   participacion    24 non-null     float64
#   3   geonombreFundar  24 non-null     object 
#  
#  |    |   vab_pb | region          |   participacion | geonombreFundar   |
#  |---:|---------:|:----------------|----------------:|:------------------|
#  | 18 |   196497 | Pampeana y AMBA |         32.5324 | Buenos Aires      |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion', k=100)
#  Index: 24 entries, 18 to 455
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   vab_pb           24 non-null     float64
#   1   region           24 non-null     object 
#   2   participacion    24 non-null     float64
#   3   geonombreFundar  24 non-null     object 
#  
#  |    |   vab_pb | region          |   participacion | geonombreFundar   |
#  |---:|---------:|:----------------|----------------:|:------------------|
#  | 18 |   196497 | Pampeana y AMBA |         32.5324 | Buenos Aires      |
#  
#  ------------------------------
#  