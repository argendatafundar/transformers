from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='prop_mundial', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12516 entries, 0 to 12515
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        12516 non-null  int64  
#   1   geocodigoFundar             12516 non-null  object 
#   2   geonombreFundar             12516 non-null  object 
#   3   exportaciones_industriales  12516 non-null  float64
#   4   prop_mundial                12516 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   exportaciones_industriales |   prop_mundial |
#  |---:|-------:|:------------------|:------------------|-----------------------------:|---------------:|
#  |  0 |   1962 | AFG               | Afganistán        |                  1.44588e+07 |    0.000159964 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_mundial', k=100)
#  RangeIndex: 12516 entries, 0 to 12515
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        12516 non-null  int64  
#   1   geocodigoFundar             12516 non-null  object 
#   2   geonombreFundar             12516 non-null  object 
#   3   exportaciones_industriales  12516 non-null  float64
#   4   prop_mundial                12516 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   exportaciones_industriales |   prop_mundial |
#  |---:|-------:|:------------------|:------------------|-----------------------------:|---------------:|
#  |  0 |   1962 | AFG               | Afganistán        |                  1.44588e+07 |      0.0159964 |
#  
#  ------------------------------
#  