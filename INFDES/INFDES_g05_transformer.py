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
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  286 non-null    object 
#   1   geonombreFundar  286 non-null    object 
#   2   anio             286 non-null    int64  
#   3   valor            286 non-null    float64
#   4   serie            286 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |    valor | serie           |
#  |---:|:------------------|:------------------|-------:|---------:|:----------------|
#  |  0 | ARG               | Argentina         |   1986 | 0.268756 | Serie empalmada |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  286 non-null    object 
#   1   geonombreFundar  286 non-null    object 
#   2   anio             286 non-null    int64  
#   3   valor            286 non-null    float64
#   4   serie            286 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | serie           |
#  |---:|:------------------|:------------------|-------:|--------:|:----------------|
#  |  0 | ARG               | Argentina         |   1986 | 26.8756 | Serie empalmada |
#  
#  ------------------------------
#  