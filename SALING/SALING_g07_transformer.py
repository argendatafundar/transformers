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
rename_cols(map={'fuente': 'indicador', 'decil': 'categoria', 'proporcion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        50 non-null     int64  
#   1   semestre    50 non-null     int64  
#   2   decil       50 non-null     object 
#   3   fuente      50 non-null     object 
#   4   proporcion  50 non-null     float64
#  
#  |    |   year |   semestre | decil   | fuente          |   proporcion |
#  |---:|-------:|-----------:|:--------|:----------------|-------------:|
#  |  0 |   2023 |          1 | Decil 1 | Ingreso laboral |     0.618709 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente': 'indicador', 'decil': 'categoria', 'proporcion': 'valor'})
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       50 non-null     int64  
#   1   semestre   50 non-null     int64  
#   2   categoria  50 non-null     object 
#   3   indicador  50 non-null     object 
#   4   valor      50 non-null     float64
#  
#  |    |   year |   semestre | categoria   | indicador       |    valor |
#  |---:|-------:|-----------:|:------------|:----------------|---------:|
#  |  0 |   2023 |          1 | Decil 1     | Ingreso laboral | 0.618709 |
#  
#  ------------------------------
#  