from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	replace_multiple_values(col='provincia', replacements={'Tierra Del Fuego': 'Tierra del Fuego', 'Santiago Del Estero': 'Santiago del Estero', 'Entre Rios': 'Entre Ríos', 'Rio Negro': 'Río Negro', 'Neuquen': 'Neuquén', 'Cordoba': 'Córdoba', 'Tucuman': 'Tucumán'})
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
#  replace_multiple_values(col='provincia', replacements={'Tierra Del Fuego': 'Tierra del Fuego', 'Santiago Del Estero': 'Santiago del Estero', 'Entre Rios': 'Entre Ríos', 'Rio Negro': 'Río Negro', 'Neuquen': 'Neuquén', 'Cordoba': 'Córdoba', 'Tucuman': 'Tucumán'})
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
#  |  0 |   2022 |             62 | Río Negro   |              15504 |          13.4383 |
#  
#  ------------------------------
#  