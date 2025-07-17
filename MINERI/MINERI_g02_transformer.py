from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
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
#  identity()
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