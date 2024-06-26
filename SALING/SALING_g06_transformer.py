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
	rename_cols(map={'proporcion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 40 entries, 0 to 39
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        40 non-null     int64  
#   1   semestre    40 non-null     int64  
#   2   proporcion  38 non-null     float64
#  
#  |    |   year |   semestre |   proporcion |
#  |---:|-------:|-----------:|-------------:|
#  |  0 |   2003 |          2 |      0.83449 |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 40 entries, 0 to 39
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        40 non-null     int64  
#   1   semestre    40 non-null     int64  
#   2   proporcion  38 non-null     float64
#   3   aniosem     40 non-null     object 
#  
#  |    |   year |   semestre |   proporcion | aniosem   |
#  |---:|-------:|-----------:|-------------:|:----------|
#  |  0 |   2003 |          2 |      0.83449 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 40 entries, 0 to 39
#  Data columns (total 2 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   proporcion  38 non-null     float64
#   1   aniosem     40 non-null     object 
#  
#  |    |   proporcion | aniosem   |
#  |---:|-------------:|:----------|
#  |  0 |      0.83449 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'proporcion': 'valor'})
#  RangeIndex: 40 entries, 0 to 39
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   valor    38 non-null     float64
#   1   aniosem  40 non-null     object 
#  
#  |    |   valor | aniosem   |
#  |---:|--------:|:----------|
#  |  0 | 0.83449 | 2003-2    |
#  
#  ------------------------------
#  