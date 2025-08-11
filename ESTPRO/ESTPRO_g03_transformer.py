from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio == anio.max()'),
	rename_cols(map={'particip_servicios_pib': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 11660 entries, 0 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         11660 non-null  object 
#   1   geonombreFundar         11660 non-null  object 
#   2   anio                    11660 non-null  int64  
#   3   es_agregacion           11660 non-null  int64  
#   4   particip_servicios_pib  10467 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion |   particip_servicios_pib |
#  |---:|:------------------|:------------------|-------:|----------------:|-------------------------:|
#  |  0 | AFG               | Afganistán        |   1970 |               0 |                 0.252995 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         220 non-null    object 
#   1   geonombreFundar         220 non-null    object 
#   2   anio                    220 non-null    int64  
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion |   particip_servicios_pib |
#  |------:|:------------------|:------------------|-------:|----------------:|-------------------------:|
#  | 11440 | AFG               | Afganistán        |   2022 |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  rename_cols(map={'particip_servicios_pib': 'valor'})
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  220 non-null    object 
#   1   geonombreFundar  220 non-null    object 
#   2   anio             220 non-null    int64  
#   3   es_agregacion    220 non-null    int64  
#   4   valor            206 non-null    float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion |   valor |
#  |------:|:------------------|:------------------|-------:|----------------:|--------:|
#  | 11440 | AFG               | Afganistán        |   2022 |               0 | 47.5201 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  220 non-null    object 
#   1   geonombreFundar  220 non-null    object 
#   2   anio             220 non-null    int64  
#   3   es_agregacion    220 non-null    int64  
#   4   valor            206 non-null    float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion |   valor |
#  |------:|:------------------|:------------------|-------:|----------------:|--------:|
#  | 11440 | AFG               | Afganistán        |   2022 |               0 | 47.5201 |
#  
#  ------------------------------
#  