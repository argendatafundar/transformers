from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def to_pandas(df: DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-'),
	drop_col(col=['year', 'semestre'], axis=1),
	rename_cols(map={'ipcf_index': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        42 non-null     int64  
#   1   semestre    42 non-null     int64  
#   2   ipcf_index  40 non-null     float64
#   3   aniosem     42 non-null     object 
#  
#  |    |   year |   semestre |   ipcf_index | aniosem   |
#  |---:|-------:|-----------:|-------------:|:----------|
#  |  0 |   2003 |          2 |          100 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        42 non-null     int64  
#   1   semestre    42 non-null     int64  
#   2   ipcf_index  40 non-null     float64
#   3   aniosem     42 non-null     object 
#  
#  |    |   year |   semestre |   ipcf_index | aniosem   |
#  |---:|-------:|-----------:|-------------:|:----------|
#  |  0 |   2003 |          2 |          100 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semestre'], axis=1)
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 2 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   ipcf_index  40 non-null     float64
#   1   aniosem     42 non-null     object 
#  
#  |    |   ipcf_index | aniosem   |
#  |---:|-------------:|:----------|
#  |  0 |          100 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'ipcf_index': 'valor'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   valor    40 non-null     float64
#   1   aniosem  42 non-null     object 
#  
#  |    |   valor | aniosem   |
#  |---:|--------:|:----------|
#  |  0 |     100 | 2003-2    |
#  
#  ------------------------------
#  