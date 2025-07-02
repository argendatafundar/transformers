from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='(anio == 1953) | (anio == anio.max())'),
	drop_col(col=['geocodigoFundar'], axis=1),
	multiplicar_por_escalar(col='participacion_vab', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    672 non-null    object 
#   1   geonombreFundar    672 non-null    object 
#   2   anio               672 non-null    int64  
#   3   region_pbg         672 non-null    object 
#   4   participacion_vab  672 non-null    float64
#   5   vab                672 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | region_pbg      |   participacion_vab |     vab |
#  |---:|:------------------|:------------------|-------:|:----------------|--------------------:|--------:|
#  |  0 | AR-B              | Buenos Aires      |   1895 | Pampeana y AMBA |             0.23917 | 4158.05 |
#  
#  ------------------------------
#  
#  query(condition='(anio == 1953) | (anio == anio.max())')
#  Index: 48 entries, 4 to 671
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    48 non-null     object 
#   1   geonombreFundar    48 non-null     object 
#   2   anio               48 non-null     int64  
#   3   region_pbg         48 non-null     object 
#   4   participacion_vab  48 non-null     float64
#   5   vab                48 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | region_pbg      |   participacion_vab |     vab |
#  |---:|:------------------|:------------------|-------:|:----------------|--------------------:|--------:|
#  |  4 | AR-B              | Buenos Aires      |   1953 | Pampeana y AMBA |            0.306406 | 36854.1 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 48 entries, 4 to 671
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    48 non-null     object 
#   1   anio               48 non-null     int64  
#   2   region_pbg         48 non-null     object 
#   3   participacion_vab  48 non-null     float64
#   4   vab                48 non-null     float64
#  
#  |    | geonombreFundar   |   anio | region_pbg      |   participacion_vab |     vab |
#  |---:|:------------------|-------:|:----------------|--------------------:|--------:|
#  |  4 | Buenos Aires      |   1953 | Pampeana y AMBA |             30.6406 | 36854.1 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion_vab', k=100)
#  Index: 48 entries, 4 to 671
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    48 non-null     object 
#   1   anio               48 non-null     int64  
#   2   region_pbg         48 non-null     object 
#   3   participacion_vab  48 non-null     float64
#   4   vab                48 non-null     float64
#  
#  |    | geonombreFundar   |   anio | region_pbg      |   participacion_vab |     vab |
#  |---:|:------------------|-------:|:----------------|--------------------:|--------:|
#  |  4 | Buenos Aires      |   1953 | Pampeana y AMBA |             30.6406 | 36854.1 |
#  
#  ------------------------------
#  