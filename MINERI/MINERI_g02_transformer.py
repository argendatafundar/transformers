from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share', k=100),
	multiplicar_por_escalar(col='tamanio_mercado', k=1e-06),
	drop_col(col=['pais', 'geocodigoFundar'], axis=1),
	wide_to_long(primary_keys=['geonombreFundar', 'mineral'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   mineral          13 non-null     object 
#   3   pais             13 non-null     object 
#   4   share            13 non-null     float64
#   5   tamanio_mercado  13 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | mineral           | pais      |    share |   tamanio_mercado |
#  |---:|:------------------|:------------------|:------------------|:----------|---------:|------------------:|
#  |  0 | AUS               | Australia         | Mineral de Hierro | Australia | 0.338462 |        3.1383e+11 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share', k=100)
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   mineral          13 non-null     object 
#   3   pais             13 non-null     object 
#   4   share            13 non-null     float64
#   5   tamanio_mercado  13 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | mineral           | pais      |   share |   tamanio_mercado |
#  |---:|:------------------|:------------------|:------------------|:----------|--------:|------------------:|
#  |  0 | AUS               | Australia         | Mineral de Hierro | Australia | 33.8462 |            313830 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tamanio_mercado', k=1e-06)
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   mineral          13 non-null     object 
#   3   pais             13 non-null     object 
#   4   share            13 non-null     float64
#   5   tamanio_mercado  13 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | mineral           | pais      |   share |   tamanio_mercado |
#  |---:|:------------------|:------------------|:------------------|:----------|--------:|------------------:|
#  |  0 | AUS               | Australia         | Mineral de Hierro | Australia | 33.8462 |            313830 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais', 'geocodigoFundar'], axis=1)
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  13 non-null     object 
#   1   mineral          13 non-null     object 
#   2   share            13 non-null     float64
#   3   tamanio_mercado  13 non-null     float64
#  
#  |    | geonombreFundar   | mineral           |   share |   tamanio_mercado |
#  |---:|:------------------|:------------------|--------:|------------------:|
#  |  0 | Australia         | Mineral de Hierro | 33.8462 |            313830 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geonombreFundar', 'mineral'], value_name='valor', var_name='indicador')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  26 non-null     object 
#   1   mineral          26 non-null     object 
#   2   indicador        26 non-null     object 
#   3   valor            26 non-null     float64
#  
#  |    | geonombreFundar   | mineral           | indicador   |   valor |
#  |---:|:------------------|:------------------|:------------|--------:|
#  |  0 | Australia         | Mineral de Hierro | share       | 33.8462 |
#  
#  ------------------------------
#  