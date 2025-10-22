from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	multiplicar_por_escalar(col='prop_industry_gdp', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10930 entries, 0 to 10929
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               10930 non-null  int64  
#   1   geocodigoFundar    10930 non-null  object 
#   2   geonombreFundar    10930 non-null  object 
#   3   industry_gdp       10930 non-null  float64
#   4   prop_industry_gdp  10930 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:------------------|:------------------|---------------:|--------------------:|
#  |  0 |   1970 | ABW               | Aruba             |    2.51723e+06 |         8.95775e-07 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 54 entries, 418 to 471
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               54 non-null     int64  
#   1   geocodigoFundar    54 non-null     object 
#   2   geonombreFundar    54 non-null     object 
#   3   industry_gdp       54 non-null     float64
#   4   prop_industry_gdp  54 non-null     float64
#  
#  |     |   anio | geocodigoFundar   | geonombreFundar   |   industry_gdp |   prop_industry_gdp |
#  |----:|-------:|:------------------|:------------------|---------------:|--------------------:|
#  | 418 |   1970 | ARG               | Argentina         |    4.50111e+10 |             1.60175 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_industry_gdp', k=100)
#  Index: 54 entries, 418 to 471
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               54 non-null     int64  
#   1   geocodigoFundar    54 non-null     object 
#   2   geonombreFundar    54 non-null     object 
#   3   industry_gdp       54 non-null     float64
#   4   prop_industry_gdp  54 non-null     float64
#  
#  |     |   anio | geocodigoFundar   | geonombreFundar   |   industry_gdp |   prop_industry_gdp |
#  |----:|-------:|:------------------|:------------------|---------------:|--------------------:|
#  | 418 |   1970 | ARG               | Argentina         |    4.50111e+10 |             1.60175 |
#  
#  ------------------------------
#  