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
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 25 entries, 0 to 24
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              25 non-null     int64  
#   1   provincia_id      25 non-null     int64  
#   2   provincia         25 non-null     object 
#   3   empleo_turistico  25 non-null     int64  
#   4   prop_turistico    25 non-null     float64
#  
#  |    |   anio |   provincia_id | provincia   |   empleo_turistico |   prop_turistico |
#  |---:|-------:|---------------:|:------------|-------------------:|-----------------:|
#  |  0 |   2022 |             62 | Rio Negro   |              15504 |          13.4383 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 25 entries, 0 to 24
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              25 non-null     int64  
#   1   provincia_id      25 non-null     int64  
#   2   provincia         25 non-null     object 
#   3   empleo_turistico  25 non-null     int64  
#   4   prop_turistico    25 non-null     float64
#  
#  |    |   anio |   provincia_id | provincia   |   empleo_turistico |   prop_turistico |
#  |---:|-------:|---------------:|:------------|-------------------:|-----------------:|
#  |  0 |   2022 |             62 | Rio Negro   |              15504 |          13.4383 |
#  
#  ------------------------------
#  