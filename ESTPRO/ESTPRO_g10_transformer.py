from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'}),
	drop_col(col=['letra', 'calificacion_cod'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 514 entries, 0 to 513
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              514 non-null    int64  
#   1   letra             514 non-null    object 
#   2   letra_desc_abrev  514 non-null    object 
#   3   calificacion_cod  514 non-null    int64  
#   4   calificacion      514 non-null    object 
#   5   particip_calif    514 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|-------:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  0 |   2016 | N       | Actividades administrativas |                  1 | Calificado     |         0.113746 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 65 entries, 7 to 513
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              65 non-null     int64  
#   1   letra             65 non-null     object 
#   2   letra_desc_abrev  65 non-null     object 
#   3   calificacion_cod  65 non-null     int64  
#   4   calificacion      65 non-null     object 
#   5   particip_calif    65 non-null     float64
#  
#  |    |   anio | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|-------:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  7 |   2023 | N       | Actividades administrativas |                  1 | Calificado     |         0.104882 |
#  
#  ------------------------------
#  
#  rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'})
#  Index: 65 entries, 7 to 513
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              65 non-null     int64  
#   1   letra             65 non-null     object 
#   2   categoria         65 non-null     object 
#   3   calificacion_cod  65 non-null     int64  
#   4   indicador         65 non-null     object 
#   5   valor             65 non-null     float64
#  
#  |    |   anio | letra   | categoria                   |   calificacion_cod | indicador   |    valor |
#  |---:|-------:|:--------|:----------------------------|-------------------:|:------------|---------:|
#  |  7 |   2023 | N       | Actividades administrativas |                  1 | Calificado  | 0.104882 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'calificacion_cod'], axis=1)
#  Index: 65 entries, 7 to 513
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       65 non-null     int64  
#   1   categoria  65 non-null     object 
#   2   indicador  65 non-null     object 
#   3   valor      65 non-null     float64
#  
#  |    |   anio | categoria                   | indicador   |   valor |
#  |---:|-------:|:----------------------------|:------------|--------:|
#  |  7 |   2023 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 65 entries, 7 to 513
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       65 non-null     int64  
#   1   categoria  65 non-null     object 
#   2   indicador  65 non-null     object 
#   3   valor      65 non-null     float64
#  
#  |    |   anio | categoria                   | indicador   |   valor |
#  |---:|-------:|:----------------------------|:------------|--------:|
#  |  7 |   2023 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  