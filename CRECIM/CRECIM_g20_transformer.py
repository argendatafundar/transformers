from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='(anio == anio.min()) or anio == anio.max()'),
	agg_sum(key_cols=['anio', 'region_pbg'], summarised_col='participacion_vab'),
	multiplicar_por_escalar(col='participacion_vab', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               672 non-null    int64  
#   1   region_pbg         672 non-null    object 
#   2   participacion_vab  672 non-null    float64
#   3   vab                672 non-null    float64
#   4   provincia_id       672 non-null    object 
#   5   geocodigoFundar    672 non-null    object 
#   6   geonombreFundar    672 non-null    object 
#  
#  |    |   anio | region_pbg      |   participacion_vab |     vab | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:----------------|--------------------:|--------:|:---------------|:------------------|:------------------|
#  |  0 |   1895 | Pampeana y AMBA |             0.23917 | 4158.05 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  query(condition='(anio == anio.min()) or anio == anio.max()')
#  Index: 48 entries, 0 to 671
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               48 non-null     int64  
#   1   region_pbg         48 non-null     object 
#   2   participacion_vab  48 non-null     float64
#   3   vab                48 non-null     float64
#   4   provincia_id       48 non-null     object 
#   5   geocodigoFundar    48 non-null     object 
#   6   geonombreFundar    48 non-null     object 
#  
#  |    |   anio | region_pbg      |   participacion_vab |     vab | provincia_id   | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:----------------|--------------------:|--------:|:---------------|:------------------|:------------------|
#  |  0 |   1895 | Pampeana y AMBA |             0.23917 | 4158.05 | AR-B           | AR-B              | Buenos Aires      |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['anio', 'region_pbg'], summarised_col='participacion_vab')
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               10 non-null     int64  
#   1   region_pbg         10 non-null     object 
#   2   participacion_vab  10 non-null     float64
#  
#  |    |   anio | region_pbg   |   participacion_vab |
#  |---:|-------:|:-------------|--------------------:|
#  |  0 |   1895 | Cuyo         |             7.19181 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion_vab', k=100)
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               10 non-null     int64  
#   1   region_pbg         10 non-null     object 
#   2   participacion_vab  10 non-null     float64
#  
#  |    |   anio | region_pbg   |   participacion_vab |
#  |---:|-------:|:-------------|--------------------:|
#  |  0 |   1895 | Cuyo         |             7.19181 |
#  
#  ------------------------------
#  