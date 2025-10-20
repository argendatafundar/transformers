from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar == 'ARG'"),
	replace_multiple_values(col='variable', replacements={'share_industrial_gdp': 'Producto', 'share_industrial_employment': 'Empleo'})
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
#  |     |   anio | geocodigoFundar   | geonombreFundar   | variable   |    valor |
#  |----:|-------:|:------------------|:------------------|:-----------|---------:|
#  | 421 |   1935 | ARG               | Argentina         | Producto   | 0.148221 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'share_industrial_gdp': 'Producto', 'share_industrial_employment': 'Empleo'})
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
#  |     |   anio | geocodigoFundar   | geonombreFundar   | variable   |    valor |
#  |----:|-------:|:------------------|:------------------|:-----------|---------:|
#  | 421 |   1935 | ARG               | Argentina         | Producto   | 0.148221 |
#  
#  ------------------------------
#  