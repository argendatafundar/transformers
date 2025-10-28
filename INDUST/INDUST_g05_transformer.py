from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['provincia_id'], axis=1),
	multiplicar_por_escalar(col='prop_industrial', k=100),
	replace_multiple_values(col='provincia', replacements={'Tierra del Fuego': 'T. del Fuego', 'Santiago del Estero': 'S. del Estero'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             480 non-null    int64  
#   1   provincia_id     480 non-null    int64  
#   2   provincia        480 non-null    object 
#   3   prop_industrial  480 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia    |   prop_industrial |
#  |---:|-------:|---------------:|:-------------|------------------:|
#  |  0 |   2004 |              6 | Buenos Aires |          0.331949 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 456 to 479
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   provincia_id     24 non-null     int64  
#   2   provincia        24 non-null     object 
#   3   prop_industrial  24 non-null     float64
#  
#  |     |   anio |   provincia_id | provincia    |   prop_industrial |
#  |----:|-------:|---------------:|:-------------|------------------:|
#  | 456 |   2023 |              6 | Buenos Aires |          0.300219 |
#  
#  ------------------------------
#  
#  drop_col(col=['provincia_id'], axis=1)
#  Index: 24 entries, 456 to 479
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   provincia        24 non-null     object 
#   2   prop_industrial  24 non-null     float64
#  
#  |     |   anio | provincia    |   prop_industrial |
#  |----:|-------:|:-------------|------------------:|
#  | 456 |   2023 | Buenos Aires |           30.0219 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_industrial', k=100)
#  Index: 24 entries, 456 to 479
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   provincia        24 non-null     object 
#   2   prop_industrial  24 non-null     float64
#  
#  |     |   anio | provincia    |   prop_industrial |
#  |----:|-------:|:-------------|------------------:|
#  | 456 |   2023 | Buenos Aires |           30.0219 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='provincia', replacements={'Tierra del Fuego': 'T. del Fuego', 'Santiago del Estero': 'S. del Estero'})
#  Index: 24 entries, 456 to 479
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             24 non-null     int64  
#   1   provincia        24 non-null     object 
#   2   prop_industrial  24 non-null     float64
#  
#  |     |   anio | provincia    |   prop_industrial |
#  |----:|-------:|:-------------|------------------:|
#  | 456 |   2023 | Buenos Aires |           30.0219 |
#  
#  ------------------------------
#  