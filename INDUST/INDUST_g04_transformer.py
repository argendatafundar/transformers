from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict, new_col:str = None) -> DataFrame:
    new_col = col if new_col is None else new_col
    df_copy = df.copy()
    df_copy[new_col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['provincia_id'], axis=1),
	multiplicar_por_escalar(col='prop_industria', k=100),
	replace_multiple_values(col='provincia', replacements={'Tierra del Fuego': 'T. del Fuego', 'Santiago del Estero': 'S. del Estero'}, new_col='provinicia_corta')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            24 non-null     int64  
#   1   provincia_id    24 non-null     int64  
#   2   provincia       24 non-null     object 
#   3   prop_industria  24 non-null     float64
#  
#  |    |   anio |   provincia_id | provincia    |   prop_industria |
#  |---:|-------:|---------------:|:-------------|-----------------:|
#  |  0 |   2023 |              6 | Buenos Aires |         0.488809 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            24 non-null     int64  
#   1   provincia_id    24 non-null     int64  
#   2   provincia       24 non-null     object 
#   3   prop_industria  24 non-null     float64
#  
#  |    |   anio |   provincia_id | provincia    |   prop_industria |
#  |---:|-------:|---------------:|:-------------|-----------------:|
#  |  0 |   2023 |              6 | Buenos Aires |         0.488809 |
#  
#  ------------------------------
#  
#  drop_col(col=['provincia_id'], axis=1)
#  Index: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            24 non-null     int64  
#   1   provincia       24 non-null     object 
#   2   prop_industria  24 non-null     float64
#  
#  |    |   anio | provincia    |   prop_industria |
#  |---:|-------:|:-------------|-----------------:|
#  |  0 |   2023 | Buenos Aires |          48.8809 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_industria', k=100)
#  Index: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            24 non-null     int64  
#   1   provincia       24 non-null     object 
#   2   prop_industria  24 non-null     float64
#  
#  |    |   anio | provincia    |   prop_industria |
#  |---:|-------:|:-------------|-----------------:|
#  |  0 |   2023 | Buenos Aires |          48.8809 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='provincia', replacements={'Tierra del Fuego': 'T. del Fuego', 'Santiago del Estero': 'S. del Estero'}, new_col='provinicia_corta')
#  Index: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              24 non-null     int64  
#   1   provincia         24 non-null     object 
#   2   prop_industria    24 non-null     float64
#   3   provinicia_corta  24 non-null     object 
#  
#  |    |   anio | provincia    |   prop_industria | provinicia_corta   |
#  |---:|-------:|:-------------|-----------------:|:-------------------|
#  |  0 |   2023 | Buenos Aires |          48.8809 | Buenos Aires       |
#  
#  ------------------------------
#  