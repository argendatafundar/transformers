from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['provincia_id'], axis=1)
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
#  |  0 |   2023 | Buenos Aires |         0.488809 |
#  
#  ------------------------------
#  