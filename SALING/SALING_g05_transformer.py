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
	rename_cols(map={'region': 'categoria', 'indice': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      336 non-null    int64  
#   1   semestre  336 non-null    int64  
#   2   region    336 non-null    object 
#   3   indice    320 non-null    float64
#  
#  |    |   year |   semestre | region   |   indice |
#  |---:|-------:|-----------:|:---------|---------:|
#  |  0 |   2003 |          2 | Total    |      100 |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      336 non-null    int64  
#   1   semestre  336 non-null    int64  
#   2   region    336 non-null    object 
#   3   indice    320 non-null    float64
#   4   aniosem   336 non-null    object 
#  
#  |    |   year |   semestre | region   |   indice | aniosem   |
#  |---:|-------:|-----------:|:---------|---------:|:----------|
#  |  0 |   2003 |          2 | Total    |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   region   336 non-null    object 
#   1   indice   320 non-null    float64
#   2   aniosem  336 non-null    object 
#  
#  |    | region   |   indice | aniosem   |
#  |---:|:---------|---------:|:----------|
#  |  0 | Total    |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'categoria', 'indice': 'valor'})
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  336 non-null    object 
#   1   valor      320 non-null    float64
#   2   aniosem    336 non-null    object 
#  
#  |    | categoria   |   valor | aniosem   |
#  |---:|:------------|--------:|:----------|
#  |  0 | Total       |     100 | 2003-2    |
#  
#  ------------------------------
#  