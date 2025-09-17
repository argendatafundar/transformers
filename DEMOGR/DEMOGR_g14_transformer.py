from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 92 entries, 0 to 91
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 92 non-null     int64  
#   1   tasa_migracion_neta  92 non-null     float64
#   2   fuente               92 non-null     object 
#  
#  |    |   anio |   tasa_migracion_neta | fuente              |
#  |---:|-------:|----------------------:|:--------------------|
#  |  0 |   1870 |               1.02304 | Lattes et al (1975) |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 92 entries, 0 to 91
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 92 non-null     int64  
#   1   tasa_migracion_neta  92 non-null     float64
#   2   fuente               92 non-null     object 
#  
#  |    |   anio |   tasa_migracion_neta | fuente              |
#  |---:|-------:|----------------------:|:--------------------|
#  |  0 |   1870 |               1.02304 | Lattes et al (1975) |
#  
#  ------------------------------
#  