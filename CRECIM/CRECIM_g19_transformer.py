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
	multiplicar_por_escalar(col='pib_pc_relativo', k=0.01)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_pbg       672 non-null    object 
#   1   anio             672 non-null    int64  
#   2   pib_pc_relativo  672 non-null    float64
#   3   provincia_id     672 non-null    object 
#   4   geocodigoFundar  672 non-null    object 
#   5   geonombreFundar  672 non-null    object 
#  
#  |    | region_pbg      |   anio |   pib_pc_relativo | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|:----------------|-------:|------------------:|:---------------|:------------------|:------------------|
#  |  0 | Pampeana y AMBA |   1895 |           102.682 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 27 to 671
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_pbg       24 non-null     object 
#   1   anio             24 non-null     int64  
#   2   pib_pc_relativo  24 non-null     float64
#   3   provincia_id     24 non-null     object 
#   4   geocodigoFundar  24 non-null     object 
#   5   geonombreFundar  24 non-null     object 
#  
#  |    | region_pbg      |   anio |   pib_pc_relativo | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|:----------------|-------:|------------------:|:---------------|:------------------|:------------------|
#  | 27 | Pampeana y AMBA |   2022 |           84.1437 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  drop_col(col=['provincia_id', 'geocodigoFundar', 'anio'], axis=1)
#  Index: 24 entries, 27 to 671
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_pbg       24 non-null     object 
#   1   pib_pc_relativo  24 non-null     float64
#   2   geonombreFundar  24 non-null     object 
#  
#  |    | region_pbg      |   pib_pc_relativo | geonombreFundar   |
#  |---:|:----------------|------------------:|:------------------|
#  | 27 | Pampeana y AMBA |          0.841437 | Buenos Aires      |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='pib_pc_relativo', k=0.01)
#  Index: 24 entries, 27 to 671
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_pbg       24 non-null     object 
#   1   pib_pc_relativo  24 non-null     float64
#   2   geonombreFundar  24 non-null     object 
#  
#  |    | region_pbg      |   pib_pc_relativo | geonombreFundar   |
#  |---:|:----------------|------------------:|:------------------|
#  | 27 | Pampeana y AMBA |          0.841437 | Buenos Aires      |
#  
#  ------------------------------
#  