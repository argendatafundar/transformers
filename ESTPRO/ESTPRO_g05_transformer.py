from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def get_anio(df, dummy = True):
    df = df.loc[(df.anio >= 2023) | (df.anio == 2019)]
    df = df.loc[df.groupby("geocodigoFundar")["anio"].idxmax()]
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	get_anio(dummy=True),
	rename_cols(map={'particip_bys_alta_intensidad': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 2470 entries, 0 to 2469
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   geocodigoFundar               2470 non-null   object 
#   1   geonombreFundar               2470 non-null   object 
#   2   anio                          2470 non-null   int64  
#   3   es_agregacion                 2470 non-null   int64  
#   4   particip_bys_alta_intensidad  2470 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar           |   anio |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|:------------------|:--------------------------|-------:|----------------:|-------------------------------:|
#  |  0 | ZSCA              | América del Sur y Central |   1995 |               1 |                       0.138143 |
#  
#  ------------------------------
#  
#  get_anio(dummy=True)
#  Index: 95 entries, 50 to 24
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   geocodigoFundar               95 non-null     object 
#   1   geonombreFundar               95 non-null     object 
#   2   anio                          95 non-null     int64  
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar                     |   anio |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|:------------------|:------------------------------------|-------:|----------------:|-------------------------------:|
#  | 50 | APEC              | Cooperación Económica Asia-Pacífico |   2019 |               1 |                       0.196705 |
#  
#  ------------------------------
#  
#  rename_cols(map={'particip_bys_alta_intensidad': 'valor'})
#  Index: 95 entries, 50 to 24
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  95 non-null     object 
#   1   geonombreFundar  95 non-null     object 
#   2   anio             95 non-null     int64  
#   3   es_agregacion    95 non-null     int64  
#   4   valor            95 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar                     |   anio |   es_agregacion |   valor |
#  |---:|:------------------|:------------------------------------|-------:|----------------:|--------:|
#  | 50 | APEC              | Cooperación Económica Asia-Pacífico |   2019 |               1 | 19.6705 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 95 entries, 50 to 24
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  95 non-null     object 
#   1   geonombreFundar  95 non-null     object 
#   2   anio             95 non-null     int64  
#   3   es_agregacion    95 non-null     int64  
#   4   valor            95 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar                     |   anio |   es_agregacion |   valor |
#  |---:|:------------------|:------------------------------------|-------:|----------------:|--------:|
#  | 50 | APEC              | Cooperación Económica Asia-Pacífico |   2019 |               1 | 19.6705 |
#  
#  ------------------------------
#  