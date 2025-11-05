from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar == 'ARG'"),
	multiplicar_por_escalar(col='tasa_desempleo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  66 non-null     object 
#   1   geonombreFundar  66 non-null     object 
#   2   anio             66 non-null     int64  
#   3   tasa_desempleo   66 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_desempleo |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | WLD               | Mundo             |   2023 |        0.0496143 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 33 entries, 33 to 65
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  33 non-null     object 
#   1   geonombreFundar  33 non-null     object 
#   2   anio             33 non-null     int64  
#   3   tasa_desempleo   33 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_desempleo |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  | 33 | ARG               | Argentina         |   2023 |            6.178 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_desempleo', k=100)
#  Index: 33 entries, 33 to 65
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  33 non-null     object 
#   1   geonombreFundar  33 non-null     object 
#   2   anio             33 non-null     int64  
#   3   tasa_desempleo   33 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_desempleo |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  | 33 | ARG               | Argentina         |   2023 |            6.178 |
#  
#  ------------------------------
#  