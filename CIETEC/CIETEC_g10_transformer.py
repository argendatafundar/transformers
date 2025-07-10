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
#   4   medida           24 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | medida                                                                       |
#  |---:|:------------------|:------------------|-------:|--------:|:-----------------------------------------------------------------------------|
#  |  0 | GTM               | Guatemala         |   2022 | 40.8574 | Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC) |
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
#   4   medida           24 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | medida                                                                       |
#  |---:|:------------------|:------------------|-------:|--------:|:-----------------------------------------------------------------------------|
#  |  0 | GTM               | Guatemala         |   2022 | 40.8574 | Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC) |
#  
#  ------------------------------
#  