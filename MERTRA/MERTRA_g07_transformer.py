from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(col='puestos_per_capita'),
	multiplicar_por_escalar(col='puestos_per_capita', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12810 entries, 0 to 12809
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     12810 non-null  object 
#   1   geonombreFundar     12810 non-null  object 
#   2   anio                12810 non-null  int64  
#   3   puestos_per_capita  9529 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   puestos_per_capita |
#  |---:|:------------------|:------------------|-------:|---------------------:|
#  |  0 | ABW               | Aruba             |   1950 |                  nan |
#  
#  ------------------------------
#  
#  drop_na(col='puestos_per_capita')
#  Index: 9529 entries, 41 to 12809
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     9529 non-null   object 
#   1   geonombreFundar     9529 non-null   object 
#   2   anio                9529 non-null   int64  
#   3   puestos_per_capita  9529 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   puestos_per_capita |
#  |---:|:------------------|:------------------|-------:|---------------------:|
#  | 41 | ABW               | Aruba             |   1991 |              45.1859 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='puestos_per_capita', k=100)
#  Index: 9529 entries, 41 to 12809
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     9529 non-null   object 
#   1   geonombreFundar     9529 non-null   object 
#   2   anio                9529 non-null   int64  
#   3   puestos_per_capita  9529 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   puestos_per_capita |
#  |---:|:------------------|:------------------|-------:|---------------------:|
#  | 41 | ABW               | Aruba             |   1991 |              45.1859 |
#  
#  ------------------------------
#  