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
	multiplicar_por_escalar(col='variacion_interanual', k=100),
	query(condition="geocodigoFundar == 'ARG'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 22082 entries, 0 to 22081
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  22082 non-null  int64  
#   1   geocodigoFundar       22082 non-null  object 
#   2   geonombreFundar       22082 non-null  object 
#   3   variable              22082 non-null  object 
#   4   variacion_interanual  22082 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | variable                                          |   variacion_interanual |
#  |---:|-------:|:------------------|:------------------|:--------------------------------------------------|-----------------------:|
#  |  0 |   1963 | AFG               | Afganistán        | Var. interanual de las importaciones industriales |               0.336099 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='variacion_interanual', k=100)
#  RangeIndex: 22082 entries, 0 to 22081
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  22082 non-null  int64  
#   1   geocodigoFundar       22082 non-null  object 
#   2   geonombreFundar       22082 non-null  object 
#   3   variable              22082 non-null  object 
#   4   variacion_interanual  22082 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | variable                                          |   variacion_interanual |
#  |---:|-------:|:------------------|:------------------|:--------------------------------------------------|-----------------------:|
#  |  0 |   1963 | AFG               | Afganistán        | Var. interanual de las importaciones industriales |                33.6099 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 122 entries, 5 to 21692
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  122 non-null    int64  
#   1   geocodigoFundar       122 non-null    object 
#   2   geonombreFundar       122 non-null    object 
#   3   variable              122 non-null    object 
#   4   variacion_interanual  122 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | variable                                          |   variacion_interanual |
#  |---:|-------:|:------------------|:------------------|:--------------------------------------------------|-----------------------:|
#  |  5 |   1963 | ARG               | Argentina         | Var. interanual de las importaciones industriales |               -37.1646 |
#  
#  ------------------------------
#  