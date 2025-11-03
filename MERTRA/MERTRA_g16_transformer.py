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
	query(condition='anio == anio.max()'),
	multiplicar_por_escalar(col='ocupado', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  756 non-null    object 
#   1   geonombreFundar  756 non-null    object 
#   2   anio             756 non-null    int64  
#   3   nivel_ed_fundar  756 non-null    object 
#   4   ocupado          756 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |---:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Hasta secundario incompleto |  0.647369 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 96 entries, 495 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  96 non-null     object 
#   1   geonombreFundar  96 non-null     object 
#   2   anio             96 non-null     int64  
#   3   nivel_ed_fundar  96 non-null     object 
#   4   ocupado          96 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='ocupado', k=100)
#  Index: 96 entries, 495 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  96 non-null     object 
#   1   geonombreFundar  96 non-null     object 
#   2   anio             96 non-null     int64  
#   3   nivel_ed_fundar  96 non-null     object 
#   4   ocupado          96 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  