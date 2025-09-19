from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP'),
	replace_value(col='fuente', curr_value='Lattes et al (1975)', new_value='Lattes (1975)')
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
#  replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP')
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
#  replace_value(col='fuente', curr_value='Lattes et al (1975)', new_value='Lattes (1975)')
#  RangeIndex: 92 entries, 0 to 91
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 92 non-null     int64  
#   1   tasa_migracion_neta  92 non-null     float64
#   2   fuente               92 non-null     object 
#  
#  |    |   anio |   tasa_migracion_neta | fuente        |
#  |---:|-------:|----------------------:|:--------------|
#  |  0 |   1870 |               1.02304 | Lattes (1975) |
#  
#  ------------------------------
#  