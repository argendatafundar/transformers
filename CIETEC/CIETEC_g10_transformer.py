from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="medida == 'Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC)'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   pais    48 non-null     object 
#   1   anio    48 non-null     int64  
#   2   valor   48 non-null     float64
#   3   medida  48 non-null     object 
#   4   iso3    48 non-null     object 
#  
#  |    | pais      |   anio |   valor | medida                                                                       | iso3   |
#  |---:|:----------|-------:|--------:|:-----------------------------------------------------------------------------|:-------|
#  |  0 | Guatemala |   2022 | 40.8574 | Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC) | GTM    |
#  
#  ------------------------------
#  
#  query(condition="medida == 'Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC)'")
#  Index: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   pais    24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   valor   24 non-null     float64
#   3   medida  24 non-null     object 
#   4   iso3    24 non-null     object 
#  
#  |    | pais      |   anio |   valor | medida                                                                       | iso3   |
#  |---:|:----------|-------:|--------:|:-----------------------------------------------------------------------------|:-------|
#  |  0 | Guatemala |   2022 | 40.8574 | Publicaciones en SCOPUS en relacion al Gasto en I+D (cada millón de U$S PPC) | GTM    |
#  
#  ------------------------------
#  