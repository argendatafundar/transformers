from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def invertir_valores(df: DataFrame, col:str) -> DataFrame:
    df[col] = df[col].apply(lambda x: 1/x if x != 0 else None)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	invertir_valores(col='ranking')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 449 entries, 0 to 448
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  449 non-null    object
#   1   geonombreFundar  449 non-null    object
#   2   anio             449 non-null    int64 
#   3   idp              449 non-null    int64 
#   4   institucion      449 non-null    object
#   5   ranking          449 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idp | institucion                                                |   ranking |
#  |---:|:------------------|:------------------|-------:|------:|:-----------------------------------------------------------|----------:|
#  |  0 | ARG               | Argentina         |   2009 | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  
#  invertir_valores(col='ranking')
#  RangeIndex: 449 entries, 0 to 448
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  449 non-null    object 
#   1   geonombreFundar  449 non-null    object 
#   2   anio             449 non-null    int64  
#   3   idp              449 non-null    int64  
#   4   institucion      449 non-null    object 
#   5   ranking          449 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idp | institucion                                                |   ranking |
#  |---:|:------------------|:------------------|-------:|------:|:-----------------------------------------------------------|----------:|
#  |  0 | ARG               | Argentina         |   2009 | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  