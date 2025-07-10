from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  24 non-null     object 
#   2   anio             24 non-null     int64  
#   3   valor            24 non-null     float64
#   4   calculo          24 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | calculo                                               |
#  |---:|:------------------|:------------------|-------:|--------:|:------------------------------------------------------|
#  |  0 | GTM               | Guatemala         |   2022 | 667.949 | Publicaciones en SCOPUS cada 100 investigadores (EJC) |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  24 non-null     object 
#   2   anio             24 non-null     int64  
#   3   valor            24 non-null     float64
#   4   calculo          24 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | calculo                                               |
#  |---:|:------------------|:------------------|-------:|--------:|:------------------------------------------------------|
#  |  0 | GTM               | Guatemala         |   2022 | 667.949 | Publicaciones en SCOPUS cada 100 investigadores (EJC) |
#  
#  ------------------------------
#  