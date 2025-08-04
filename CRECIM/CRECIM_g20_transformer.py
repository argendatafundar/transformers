from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	agg_sum(key_cols=['anio', 'region_pbg'], summarised_col='participacion_vab'),
	multiplicar_por_escalar(col='participacion_vab', k=100),
	query(condition='anio in [1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2015, 2022]')
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
#  agg_sum(key_cols=['anio', 'region_pbg'], summarised_col='participacion_vab')
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               140 non-null    int64  
#   1   region_pbg         140 non-null    object 
#   2   participacion_vab  140 non-null    float64
#  
#  |    |   anio | region_pbg   |   participacion_vab |
#  |---:|-------:|:-------------|--------------------:|
#  |  0 |   1895 | Cuyo         |           0.0719181 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion_vab', k=100)
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               140 non-null    int64  
#   1   region_pbg         140 non-null    object 
#   2   participacion_vab  140 non-null    float64
#  
#  |    |   anio | region_pbg   |   participacion_vab |
#  |---:|-------:|:-------------|--------------------:|
#  |  0 |   1895 | Cuyo         |             7.19181 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2015, 2022]')
#  Index: 60 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               60 non-null     int64  
#   1   region_pbg         60 non-null     object 
#   2   participacion_vab  60 non-null     float64
#  
#  |    |   anio | region_pbg   |   participacion_vab |
#  |---:|-------:|:-------------|--------------------:|
#  |  0 |   1895 | Cuyo         |             7.19181 |
#  
#  ------------------------------
#  