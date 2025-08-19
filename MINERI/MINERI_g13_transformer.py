from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	query(condition="empleo_base == 'empleo_minero_siacam'"),
	drop_col(col='empleo_base', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             81 non-null     int64  
#   1   empleo_minero_perc_formal_total  81 non-null     float64
#   2   empleo_base                      81 non-null     object 
#   3   cantidad_puestos                 81 non-null     float64
#  
#  |    |   anio |   empleo_minero_perc_formal_total | empleo_base       |   cantidad_puestos |
#  |---:|-------:|----------------------------------:|:------------------|-------------------:|
#  |  0 |   1996 |                          0.383496 | suma_mineria_oede |            10283.8 |
#  
#  ------------------------------
#  
#  query(condition="empleo_base == 'empleo_minero_siacam'")
#  Index: 27 entries, 1 to 79
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             27 non-null     int64  
#   1   empleo_minero_perc_formal_total  27 non-null     float64
#   2   empleo_base                      27 non-null     object 
#   3   cantidad_puestos                 27 non-null     float64
#  
#  |    |   anio |   empleo_minero_perc_formal_total | empleo_base          |   cantidad_puestos |
#  |---:|-------:|----------------------------------:|:---------------------|-------------------:|
#  |  1 |   1996 |                          0.383496 | empleo_minero_siacam |            13465.3 |
#  
#  ------------------------------
#  
#  drop_col(col='empleo_base', axis=1)
#  Index: 27 entries, 1 to 79
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             27 non-null     int64  
#   1   empleo_minero_perc_formal_total  27 non-null     float64
#   2   cantidad_puestos                 27 non-null     float64
#  
#  |    |   anio |   empleo_minero_perc_formal_total |   cantidad_puestos |
#  |---:|-------:|----------------------------------:|-------------------:|
#  |  1 |   1996 |                          0.383496 |            13465.3 |
#  
#  ------------------------------
#  