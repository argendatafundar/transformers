from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='cantidad_turistas', k=0.001),
	query(condition='anio >= 2012 & anio <= 2023')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               28 non-null     int64  
#   1   tipo_turismo       28 non-null     object 
#   2   cantidad_turistas  28 non-null     float64
#  
#  |    |   anio | tipo_turismo   |   cantidad_turistas |
#  |---:|-------:|:---------------|--------------------:|
#  |  0 |   2010 | Receptivo      |         6.73859e+06 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='cantidad_turistas', k=0.001)
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               28 non-null     int64  
#   1   tipo_turismo       28 non-null     object 
#   2   cantidad_turistas  28 non-null     float64
#  
#  |    |   anio | tipo_turismo   |   cantidad_turistas |
#  |---:|-------:|:---------------|--------------------:|
#  |  0 |   2010 | Receptivo      |             6738.59 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 2012 & anio <= 2023')
#  Index: 24 entries, 2 to 26
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               24 non-null     int64  
#   1   tipo_turismo       24 non-null     object 
#   2   cantidad_turistas  24 non-null     float64
#  
#  |    |   anio | tipo_turismo   |   cantidad_turistas |
#  |---:|-------:|:---------------|--------------------:|
#  |  2 |   2012 | Receptivo      |             6461.77 |
#  
#  ------------------------------
#  