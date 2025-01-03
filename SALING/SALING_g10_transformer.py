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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-'),
	drop_col(col=['year', 'semestre'], axis=1),
	rename_cols(map={'fuente': 'categoria', 'indice': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      168 non-null    int64  
#   1   semestre  168 non-null    int64  
#   2   fuente    168 non-null    object 
#   3   indice    160 non-null    float64
#  
#  |    |   year |   semestre | fuente             |   indice |
#  |---:|-------:|-----------:|:-------------------|---------:|
#  |  0 |   2003 |          2 | Ingreso de capital |      100 |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      168 non-null    int64  
#   1   semestre  168 non-null    int64  
#   2   fuente    168 non-null    object 
#   3   indice    160 non-null    float64
#   4   aniosem   168 non-null    object 
#  
#  |    |   year |   semestre | fuente             |   indice | aniosem   |
#  |---:|-------:|-----------:|:-------------------|---------:|:----------|
#  |  0 |   2003 |          2 | Ingreso de capital |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   fuente   168 non-null    object 
#   1   indice   160 non-null    float64
#   2   aniosem  168 non-null    object 
#  
#  |    | fuente             |   indice | aniosem   |
#  |---:|:-------------------|---------:|:----------|
#  |  0 | Ingreso de capital |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente': 'categoria', 'indice': 'valor'})
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  168 non-null    object 
#   1   valor      160 non-null    float64
#   2   aniosem    168 non-null    object 
#  
#  |    | categoria          |   valor | aniosem   |
#  |---:|:-------------------|--------:|:----------|
#  |  0 | Ingreso de capital |     100 | 2003-2    |
#  
#  ------------------------------
#  