from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'fuente': 'indicador', 'edad_jefe': 'categoria', 'proporcion': 'valor'})
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
#  |  0 |   2023 |          1 | 24 o menos  | Ingreso laboral |     0.714832 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente': 'indicador', 'edad_jefe': 'categoria', 'proporcion': 'valor'})
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       30 non-null     int64  
#   1   semestre   30 non-null     int64  
#   2   categoria  30 non-null     object 
#   3   indicador  30 non-null     object 
#   4   valor      30 non-null     float64
#  
#  |    |   year |   semestre | categoria   | indicador       |    valor |
#  |---:|-------:|-----------:|:------------|:----------------|---------:|
#  |  0 |   2023 |          1 | 24 o menos  | Ingreso laboral | 0.714832 |
#  
#  ------------------------------
#  