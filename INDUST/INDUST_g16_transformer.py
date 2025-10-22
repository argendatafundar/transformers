from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar == 'ARG'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 25074 entries, 0 to 25073
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                25074 non-null  int64  
#   1   geocodigoFundar     25074 non-null  object 
#   2   geonombreFundar     25032 non-null  object 
#   3   flujo               25074 non-null  object 
#   4   valores_corrientes  25074 non-null  float64
#   5   valores_constantes  25074 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | flujo                      |   valores_corrientes |   valores_constantes |
#  |---:|-------:|:------------------|:------------------|:---------------------------|---------------------:|---------------------:|
#  |  0 |   1962 | AFG               | Afganistán        | Exportaciones industriales |              14.4588 |              111.638 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 124 entries, 10 to 24637
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                124 non-null    int64  
#   1   geocodigoFundar     124 non-null    object 
#   2   geonombreFundar     124 non-null    object 
#   3   flujo               124 non-null    object 
#   4   valores_corrientes  124 non-null    float64
#   5   valores_constantes  124 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | flujo                      |   valores_corrientes |   valores_constantes |
#  |---:|-------:|:------------------|:------------------|:---------------------------|---------------------:|---------------------:|
#  | 10 |   1962 | ARG               | Argentina         | Exportaciones industriales |              234.905 |              1813.73 |
#  
#  ------------------------------
#  