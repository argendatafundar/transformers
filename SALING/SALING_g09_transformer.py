from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-'),
	sort_values(how='ascending', by=['year', 'semestre'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        80 non-null     int64  
#   1   semestre    80 non-null     int64  
#   2   genero      80 non-null     object 
#   3   proporcion  76 non-null     float64
#  
#  |    |   year |   semestre | genero   |   proporcion |
#  |---:|-------:|-----------:|:---------|-------------:|
#  |  0 |   2003 |          2 | Mujer    |     0.727663 |
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
#  sort_values(how='ascending', by=['year', 'semestre'])
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