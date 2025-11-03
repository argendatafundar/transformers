from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def imput_na(df:DataFrame, bool_mask:list[bool], col:str, value:Any):
    df.loc[bool_mask,col] = value
    return df

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
	query(condition='anio == anio.max()'),
	query(condition="apertura_edad.isin(['Edad, entre 18 y 65','Edad, total'])"),
	imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, False, False], col='geonombreFundar', value='Argentina'),
	multiplicar_por_escalar(col='tasa_empleo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 788 entries, 0 to 787
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  756 non-null    object 
#   1   geonombreFundar  756 non-null    object 
#   2   anio             788 non-null    int64  
#   3   apertura_edad    788 non-null    object 
#   4   tasa_empleo      788 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | apertura_edad       |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|:--------------------|--------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Edad, entre 18 y 65 |      0.656322 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 100 entries, 688 to 787
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  96 non-null     object 
#   1   geonombreFundar  96 non-null     object 
#   2   anio             100 non-null    int64  
#   3   apertura_edad    100 non-null    object 
#   4   tasa_empleo      100 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | apertura_edad       |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:--------------------|--------------:|
#  | 688 | AR-B              | Buenos Aires      |   2023 | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  query(condition="apertura_edad.isin(['Edad, entre 18 y 65','Edad, total'])")
#  Index: 50 entries, 688 to 787
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  50 non-null     object 
#   2   anio             50 non-null     int64  
#   3   apertura_edad    50 non-null     object 
#   4   tasa_empleo      50 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | apertura_edad       |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:--------------------|--------------:|
#  | 688 | AR-B              | Buenos Aires      |   2023 | Edad, entre 18 y 65 |       71.4799 |
#  
#  ------------------------------
#  
#  imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, False, False], col='geonombreFundar', value='Argentina')
#  Index: 50 entries, 688 to 787
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  50 non-null     object 
#   2   anio             50 non-null     int64  
#   3   apertura_edad    50 non-null     object 
#   4   tasa_empleo      50 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | apertura_edad       |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:--------------------|--------------:|
#  | 688 | AR-B              | Buenos Aires      |   2023 | Edad, entre 18 y 65 |       71.4799 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo', k=100)
#  Index: 50 entries, 688 to 787
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  50 non-null     object 
#   2   anio             50 non-null     int64  
#   3   apertura_edad    50 non-null     object 
#   4   tasa_empleo      50 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | apertura_edad       |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:--------------------|--------------:|
#  | 688 | AR-B              | Buenos Aires      |   2023 | Edad, entre 18 y 65 |       71.4799 |
#  
#  ------------------------------
#  