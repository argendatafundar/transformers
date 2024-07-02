from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-'),
	drop_col(col=['year', 'semestre'], axis=1),
	rename_cols(map={'genero': 'categoria', 'proporcion': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        80 non-null     int64  
#   1   semestre    80 non-null     int64  
#   2   genero      80 non-null     object 
#   3   proporcion  76 non-null     float64
#   4   aniosem     80 non-null     object 
#  
#  |    |   year |   semestre | genero   |   proporcion | aniosem   |
#  |---:|-------:|-----------:|:---------|-------------:|:----------|
#  |  0 |   2003 |          2 | Mujer    |     0.727663 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        80 non-null     int64  
#   1   semestre    80 non-null     int64  
#   2   genero      80 non-null     object 
#   3   proporcion  76 non-null     float64
#   4   aniosem     80 non-null     object 
#  
#  |    |   year |   semestre | genero   |   proporcion | aniosem   |
#  |---:|-------:|-----------:|:---------|-------------:|:----------|
#  |  0 |   2003 |          2 | Mujer    |     0.727663 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   genero      80 non-null     object 
#   1   proporcion  76 non-null     float64
#   2   aniosem     80 non-null     object 
#  
#  |    | genero   |   proporcion | aniosem   |
#  |---:|:---------|-------------:|:----------|
#  |  0 | Mujer    |     0.727663 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'genero': 'categoria', 'proporcion': 'valor'})
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  80 non-null     object 
#   1   valor      76 non-null     float64
#   2   aniosem    80 non-null     object 
#  
#  |    | categoria   |   valor | aniosem   |
#  |---:|:------------|--------:|:----------|
#  |  0 | Mujer       | 72.7663 | 2003-2    |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  80 non-null     object 
#   1   valor      76 non-null     float64
#   2   aniosem    80 non-null     object 
#  
#  |    | categoria   |   valor | aniosem   |
#  |---:|:------------|--------:|:----------|
#  |  0 | Mujer       | 72.7663 | 2003-2    |
#  
#  ------------------------------
#  