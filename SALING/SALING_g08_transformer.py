from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='proporcion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        30 non-null     int64  
#   1   semestre    30 non-null     int64  
#   2   edad_jefe   30 non-null     object 
#   3   fuente      30 non-null     object 
#   4   proporcion  30 non-null     float64
#  
#  |    |   year |   semestre | edad_jefe   | fuente          |   proporcion |
#  |---:|-------:|-----------:|:------------|:----------------|-------------:|
#  |  0 |   2024 |          1 | 24 o menos  | Ingreso laboral |     0.742226 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='proporcion', k=100)
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        30 non-null     int64  
#   1   semestre    30 non-null     int64  
#   2   edad_jefe   30 non-null     object 
#   3   fuente      30 non-null     object 
#   4   proporcion  30 non-null     float64
#  
#  |    |   year |   semestre | edad_jefe   | fuente          |   proporcion |
#  |---:|-------:|-----------:|:------------|:----------------|-------------:|
#  |  0 |   2024 |          1 | 24 o menos  | Ingreso laboral |      74.2226 |
#  
#  ------------------------------
#  