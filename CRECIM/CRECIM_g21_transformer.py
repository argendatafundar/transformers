from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	drop_col(col=['provincia_id', 'geocodigoFundar'], axis=1),
	multiplicar_por_escalar(col='var_participacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   region_pbg         24 non-null     object 
#   1   var_participacion  24 non-null     float64
#   2   provincia_id       24 non-null     object 
#   3   geocodigoFundar    24 non-null     object 
#   4   geonombreFundar    24 non-null     object 
#  
#  |    | region_pbg      |   var_participacion | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|:----------------|--------------------:|:---------------|:------------------|:------------------|
#  |  0 | Pampeana y AMBA |            0.018752 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  drop_col(col=['provincia_id', 'geocodigoFundar'], axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   region_pbg         24 non-null     object 
#   1   var_participacion  24 non-null     float64
#   2   geonombreFundar    24 non-null     object 
#  
#  |    | region_pbg      |   var_participacion | geonombreFundar   |
#  |---:|:----------------|--------------------:|:------------------|
#  |  0 | Pampeana y AMBA |              1.8752 | Buenos Aires      |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='var_participacion', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   region_pbg         24 non-null     object 
#   1   var_participacion  24 non-null     float64
#   2   geonombreFundar    24 non-null     object 
#  
#  |    | region_pbg      |   var_participacion | geonombreFundar   |
#  |---:|:----------------|--------------------:|:------------------|
#  |  0 | Pampeana y AMBA |              1.8752 | Buenos Aires      |
#  
#  ------------------------------
#  