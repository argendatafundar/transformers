from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def completar_combinaciones(df:DataFrame, keys:list[str]):

    import pandas as pd

    niveles = [df[key].dropna().unique() for key in keys]
    combinaciones = pd.MultiIndex.from_product(niveles, names=keys).to_frame(index=False)
    df = combinaciones.merge(df, on=keys, how='left')
    return df

@transformer.convert
def invertir_valores(df: DataFrame, col:str) -> DataFrame:
    import numpy as np  
    df[col] = df[col].apply(lambda x: 1/x if x not in [0, np.nan] else np.nan)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geonombreFundar', 'geocodigoFundar', 'idp'], axis=1),
	completar_combinaciones(keys=['anio', 'institucion']),
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
#  drop_col(col=['geonombreFundar', 'geocodigoFundar', 'idp'], axis=1)
#  RangeIndex: 449 entries, 0 to 448
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         449 non-null    int64 
#   1   institucion  449 non-null    object
#   2   ranking      449 non-null    int64 
#  
#  |    |   anio | institucion                                                |   ranking |
#  |---:|-------:|:-----------------------------------------------------------|----------:|
#  |  0 |   2009 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  
#  completar_combinaciones(keys=['anio', 'institucion'])
#  RangeIndex: 768 entries, 0 to 767
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         768 non-null    int64  
#   1   institucion  768 non-null    object 
#   2   ranking      449 non-null    float64
#  
#  |    |   anio | institucion                                                |   ranking |
#  |---:|-------:|:-----------------------------------------------------------|----------:|
#  |  0 |   2009 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  
#  invertir_valores(col='ranking')
#  RangeIndex: 768 entries, 0 to 767
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         768 non-null    int64  
#   1   institucion  768 non-null    object 
#   2   ranking      449 non-null    float64
#  
#  |    |   anio | institucion                                                |   ranking |
#  |---:|-------:|:-----------------------------------------------------------|----------:|
#  |  0 |   2009 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  