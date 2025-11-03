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

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False], col='geonombreFundar', value='Argentina'),
	replace_value(col='sexo', curr_value='Varon', new_value='Varón', mapping=None),
	query(condition="geonombreFundar != 'Argentina'"),
	query(condition='anio == anio.max()'),
	query(condition="sexo ! = 'Total'"),
	multiplicar_por_escalar(col='tasa_empleo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 591 entries, 0 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  567 non-null    object 
#   1   geonombreFundar  567 non-null    object 
#   2   anio             591 non-null    int64  
#   3   sexo             591 non-null    object 
#   4   tasa_empleo      591 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|:-------|--------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Mujer  |      0.542051 |
#  
#  ------------------------------
#  
#  imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, False, False, False], col='geonombreFundar', value='Argentina')
#  RangeIndex: 591 entries, 0 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  567 non-null    object 
#   1   geonombreFundar  591 non-null    object 
#   2   anio             591 non-null    int64  
#   3   sexo             591 non-null    object 
#   4   tasa_empleo      591 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|:-------|--------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Mujer  |      0.542051 |
#  
#  ------------------------------
#  
#  replace_value(col='sexo', curr_value='Varon', new_value='Varón', mapping=None)
#  RangeIndex: 591 entries, 0 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  567 non-null    object 
#   1   geonombreFundar  591 non-null    object 
#   2   anio             591 non-null    int64  
#   3   sexo             591 non-null    object 
#   4   tasa_empleo      591 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|:-------|--------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Mujer  |      0.542051 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar != 'Argentina'")
#  Index: 567 entries, 0 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  567 non-null    object 
#   1   geonombreFundar  567 non-null    object 
#   2   anio             567 non-null    int64  
#   3   sexo             567 non-null    object 
#   4   tasa_empleo      567 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|:-------|--------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Mujer  |      0.542051 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 72 entries, 516 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  72 non-null     object 
#   1   geonombreFundar  72 non-null     object 
#   2   anio             72 non-null     int64  
#   3   sexo             72 non-null     object 
#   4   tasa_empleo      72 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:-------|--------------:|
#  | 516 | AR-B              | Buenos Aires      |   2023 | Mujer  |      0.625285 |
#  
#  ------------------------------
#  
#  query(condition="sexo ! = 'Total'")
#  Index: 48 entries, 516 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  48 non-null     object 
#   2   anio             48 non-null     int64  
#   3   sexo             48 non-null     object 
#   4   tasa_empleo      48 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:-------|--------------:|
#  | 516 | AR-B              | Buenos Aires      |   2023 | Mujer  |       62.5285 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo', k=100)
#  Index: 48 entries, 516 to 590
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  48 non-null     object 
#   2   anio             48 non-null     int64  
#   3   sexo             48 non-null     object 
#   4   tasa_empleo      48 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|:-------|--------------:|
#  | 516 | AR-B              | Buenos Aires      |   2023 | Mujer  |       62.5285 |
#  
#  ------------------------------
#  