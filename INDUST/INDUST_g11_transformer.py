from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict, new_col:str = None) -> DataFrame:
    new_col = col if new_col is None else new_col
    df_copy = df.copy()
    df_copy[new_col] = df_copy[col].replace(replacements)
    return df_copy

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
	query(condition="geocodigoFundar == 'ARG'"),
	replace_multiple_values(col='variable', replacements={'share_industrial_gdp': 'PIB', 'share_industrial_employment': 'Empleo'}, new_col=None),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13518 entries, 0 to 13517
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             13518 non-null  int64  
#   1   geocodigoFundar  13518 non-null  object 
#   2   geonombreFundar  13484 non-null  object 
#   3   variable         13518 non-null  object 
#   4   valor            13518 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | variable             |     valor |
#  |---:|-------:|:------------------|:------------------|:---------------------|----------:|
#  |  0 |   1970 | ABW               | Aruba             | share_industrial_gdp | 0.0169678 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 163 entries, 421 to 583
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             163 non-null    int64  
#   1   geocodigoFundar  163 non-null    object 
#   2   geonombreFundar  163 non-null    object 
#   3   variable         163 non-null    object 
#   4   valor            163 non-null    float64
#  
#  |     |   anio | geocodigoFundar   | geonombreFundar   | variable             |    valor |
#  |----:|-------:|:------------------|:------------------|:---------------------|---------:|
#  | 421 |   1935 | ARG               | Argentina         | share_industrial_gdp | 0.148221 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'share_industrial_gdp': 'PIB', 'share_industrial_employment': 'Empleo'}, new_col=None)
#  Index: 163 entries, 421 to 583
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             163 non-null    int64  
#   1   geocodigoFundar  163 non-null    object 
#   2   geonombreFundar  163 non-null    object 
#   3   variable         163 non-null    object 
#   4   valor            163 non-null    float64
#  
#  |     |   anio | geocodigoFundar   | geonombreFundar   | variable   |   valor |
#  |----:|-------:|:------------------|:------------------|:-----------|--------:|
#  | 421 |   1935 | ARG               | Argentina         | PIB        | 14.8221 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 163 entries, 421 to 583
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             163 non-null    int64  
#   1   geocodigoFundar  163 non-null    object 
#   2   geonombreFundar  163 non-null    object 
#   3   variable         163 non-null    object 
#   4   valor            163 non-null    float64
#  
#  |     |   anio | geocodigoFundar   | geonombreFundar   | variable   |   valor |
#  |----:|-------:|:------------------|:------------------|:-----------|--------:|
#  | 421 |   1935 | ARG               | Argentina         | PIB        | 14.8221 |
#  
#  ------------------------------
#  