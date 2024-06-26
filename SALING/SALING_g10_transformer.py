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
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      160 non-null    int64  
#   1   semestre  160 non-null    int64  
#   2   fuente    160 non-null    object 
#   3   indice    152 non-null    float64
#  
#  |    |   year |   semestre | fuente             |   indice |
#  |---:|-------:|-----------:|:-------------------|---------:|
#  |  0 |   2003 |          2 | Ingreso de capital |      100 |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      160 non-null    int64  
#   1   semestre  160 non-null    int64  
#   2   fuente    160 non-null    object 
#   3   indice    152 non-null    float64
#   4   aniosem   160 non-null    object 
#  
#  |    |   year |   semestre | fuente             |   indice | aniosem   |
#  |---:|-------:|-----------:|:-------------------|---------:|:----------|
#  |  0 |   2003 |          2 | Ingreso de capital |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   fuente   160 non-null    object 
#   1   indice   152 non-null    float64
#   2   aniosem  160 non-null    object 
#  
#  |    | fuente             |   indice | aniosem   |
#  |---:|:-------------------|---------:|:----------|
#  |  0 | Ingreso de capital |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente': 'categoria', 'indice': 'valor'})
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  160 non-null    object 
#   1   valor      152 non-null    float64
#   2   aniosem    160 non-null    object 
#  
#  |    | categoria          |   valor | aniosem   |
#  |---:|:-------------------|--------:|:----------|
#  |  0 | Ingreso de capital |     100 | 2003-2    |
#  
#  ------------------------------
#  