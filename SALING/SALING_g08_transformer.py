from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'fuente': 'indicador', 'edad_jefe': 'categoria', 'proporcion': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	drop_col(col=['year', 'semestre'], axis=1),
	replace_value(col='categoria', curr_value='65 o mas', new_value='65 o más')
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
#  |    |   year |   semestre | categoria   | indicador       |   valor |
#  |---:|-------:|-----------:|:------------|:----------------|--------:|
#  |  0 |   2024 |          1 | 24 o menos  | Ingreso laboral | 74.2226 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
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
#  |    |   year |   semestre | categoria   | indicador       |   valor |
#  |---:|-------:|-----------:|:------------|:----------------|--------:|
#  |  0 |   2024 |          1 | 24 o menos  | Ingreso laboral | 74.2226 |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  30 non-null     object 
#   1   indicador  30 non-null     object 
#   2   valor      30 non-null     float64
#  
#  |    | categoria   | indicador       |   valor |
#  |---:|:------------|:----------------|--------:|
#  |  0 | 24 o menos  | Ingreso laboral | 74.2226 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='65 o mas', new_value='65 o más')
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  30 non-null     object 
#   1   indicador  30 non-null     object 
#   2   valor      30 non-null     float64
#  
#  |    | categoria   | indicador       |   valor |
#  |---:|:------------|:----------------|--------:|
#  |  0 | 24 o menos  | Ingreso laboral | 74.2226 |
#  
#  ------------------------------
#  