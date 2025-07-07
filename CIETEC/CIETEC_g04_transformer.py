from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def completar_combinaciones(df:DataFrame, keys:list[str]):

    import pandas as pd

    niveles = [df[key].dropna().unique() for key in keys]
    combinaciones = pd.MultiIndex.from_product(niveles, names=keys).to_frame(index=False)
    df = combinaciones.merge(df, on=keys, how='left')
    return df

@transformer.convert
def reverse_ranking(df:DataFrame):
    df["reversed_ranking"] = df.ranking.apply(lambda x: 48 - x + 1)
    return df

@transformer.convert
def fill_na(df:DataFrame, col:str, fill:Any):
    df[col] = df[col].fillna(fill)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	completar_combinaciones(keys=['ranking', 'anio']),
	reverse_ranking(),
	fill_na(col='institucion', fill='')
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
#  completar_combinaciones(keys=['ranking', 'anio'])
#  RangeIndex: 768 entries, 0 to 767
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   ranking           768 non-null    int64  
#   1   anio              768 non-null    int64  
#   2   geocodigoFundar   449 non-null    object 
#   3   geonombreFundar   449 non-null    object 
#   4   idp               449 non-null    float64
#   5   institucion       768 non-null    object 
#   6   reversed_ranking  768 non-null    int64  
#  
#  |    |   ranking |   anio | geocodigoFundar   | geonombreFundar   |   idp | institucion                                                |   reversed_ranking |
#  |---:|----------:|-------:|:------------------|:------------------|------:|:-----------------------------------------------------------|-------------------:|
#  |  0 |         1 |   2009 | ARG               | Argentina         | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |                 48 |
#  
#  ------------------------------
#  
#  reverse_ranking()
#  RangeIndex: 768 entries, 0 to 767
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   ranking           768 non-null    int64  
#   1   anio              768 non-null    int64  
#   2   geocodigoFundar   449 non-null    object 
#   3   geonombreFundar   449 non-null    object 
#   4   idp               449 non-null    float64
#   5   institucion       768 non-null    object 
#   6   reversed_ranking  768 non-null    int64  
#  
#  |    |   ranking |   anio | geocodigoFundar   | geonombreFundar   |   idp | institucion                                                |   reversed_ranking |
#  |---:|----------:|-------:|:------------------|:------------------|------:|:-----------------------------------------------------------|-------------------:|
#  |  0 |         1 |   2009 | ARG               | Argentina         | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |                 48 |
#  
#  ------------------------------
#  
#  fill_na(col='institucion', fill='')
#  RangeIndex: 768 entries, 0 to 767
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   ranking           768 non-null    int64  
#   1   anio              768 non-null    int64  
#   2   geocodigoFundar   449 non-null    object 
#   3   geonombreFundar   449 non-null    object 
#   4   idp               449 non-null    float64
#   5   institucion       768 non-null    object 
#   6   reversed_ranking  768 non-null    int64  
#  
#  |    |   ranking |   anio | geocodigoFundar   | geonombreFundar   |   idp | institucion                                                |   reversed_ranking |
#  |---:|----------:|-------:|:------------------|:------------------|------:|:-----------------------------------------------------------|-------------------:|
#  |  0 |         1 |   2009 | ARG               | Argentina         | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |                 48 |
#  
#  ------------------------------
#  