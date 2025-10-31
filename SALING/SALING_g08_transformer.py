from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='proporcion', k=100),
	replace_multiple_values(col='fuente', replacements={'Ingreso laboral': 'Laboral', 'Ingreso de capital': 'Capital', 'Ingreso de jubilaciones': 'Jubilaciones', 'Ingreso de transferencias estatales': 'Transferencias estatales', 'Otros ingresos': 'Otros ingresos'}),
	ordenar_dos_columnas(col1='edad_jefe', order1=['65 o mas55 a 64', '35 a 44', '45 a 54', '25 a 34', '24 o menos'], col2='fuente', order2=['Laboral', 'Capital', 'Jubilaciones', 'Transferencias estatales', 'Otros ingresos'])
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
#  replace_multiple_values(col='fuente', replacements={'Ingreso laboral': 'Laboral', 'Ingreso de capital': 'Capital', 'Ingreso de jubilaciones': 'Jubilaciones', 'Ingreso de transferencias estatales': 'Transferencias estatales', 'Otros ingresos': 'Otros ingresos'})
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   year        30 non-null     int64   
#   1   semestre    30 non-null     int64   
#   2   edad_jefe   20 non-null     category
#   3   fuente      30 non-null     category
#   4   proporcion  30 non-null     float64 
#  
#  |    |   year |   semestre | edad_jefe   | fuente   |   proporcion |
#  |---:|-------:|-----------:|:------------|:---------|-------------:|
#  |  0 |   2024 |          1 | 24 o menos  | Laboral  |      74.2226 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='edad_jefe', order1=['65 o mas55 a 64', '35 a 44', '45 a 54', '25 a 34', '24 o menos'], col2='fuente', order2=['Laboral', 'Capital', 'Jubilaciones', 'Transferencias estatales', 'Otros ingresos'])
#  Index: 30 entries, 2 to 29
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   year        30 non-null     int64   
#   1   semestre    30 non-null     int64   
#   2   edad_jefe   20 non-null     category
#   3   fuente      30 non-null     category
#   4   proporcion  30 non-null     float64 
#  
#  |    |   year |   semestre | edad_jefe   | fuente   |   proporcion |
#  |---:|-------:|-----------:|:------------|:---------|-------------:|
#  |  2 |   2024 |          1 | 35 a 44     | Laboral  |      88.2807 |
#  
#  ------------------------------
#  