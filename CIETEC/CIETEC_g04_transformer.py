from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def reverse_ranking(df:DataFrame):
    df["reversed_ranking"] = df.ranking.apply(lambda x: 48 - x + 1)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	reverse_ranking()
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
#  reverse_ranking()
#  RangeIndex: 449 entries, 0 to 448
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   449 non-null    object
#   1   geonombreFundar   449 non-null    object
#   2   anio              449 non-null    int64 
#   3   idp               449 non-null    int64 
#   4   institucion       449 non-null    object
#   5   ranking           449 non-null    int64 
#   6   reversed_ranking  449 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idp | institucion                                                |   ranking |   reversed_ranking |
#  |---:|:------------------|:------------------|-------:|------:|:-----------------------------------------------------------|----------:|-------------------:|
#  |  0 | ARG               | Argentina         |   2009 | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |                 48 |
#  
#  ------------------------------
#  