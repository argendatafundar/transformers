from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def regreplace_text(df: DataFrame, col:str, pattern:str, replacement:str):
    import re
    df[col] = df[col].apply(lambda x: re.sub(pattern, replacement, x))
    return df

@transformer.convert
def regreplace_text(df: DataFrame, col:str, pattern:str, replacement:str):
    import re
    df[col] = df[col].apply(lambda x: re.sub(pattern, replacement, x))
    return df

@transformer.convert
def regreplace_text(df: DataFrame, col:str, pattern:str, replacement:str):
    import re
    df[col] = df[col].apply(lambda x: re.sub(pattern, replacement, x))
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
regreplace_text(col='campania', pattern='\\d{4}/', replacement=''),
	regreplace_text(col='campania', pattern='^([7-9]\\d{1})$', replacement='19\\1'),
	regreplace_text(col='campania', pattern='^([0-2]\\d{1})$', replacement='20\\1'),
	rename_cols(map={'cultivo': 'categoria', 'rindes_avg': 'valor', 'campania': 'anio'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   cultivo     162 non-null    object 
#   1   campania    162 non-null    object 
#   2   rindes_avg  161 non-null    float64
#  
#  |    | cultivo   | campania   |   rindes_avg |
#  |---:|:----------|:-----------|-------------:|
#  |  0 | Maíz      | 1969/70    |       2330.5 |
#  
#  ------------------------------
#  
#  regreplace_text(col='campania', pattern='\\d{4}/', replacement='')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   cultivo     162 non-null    object 
#   1   campania    162 non-null    object 
#   2   rindes_avg  161 non-null    float64
#  
#  |    | cultivo   |   campania |   rindes_avg |
#  |---:|:----------|-----------:|-------------:|
#  |  0 | Maíz      |       1970 |       2330.5 |
#  
#  ------------------------------
#  
#  regreplace_text(col='campania', pattern='^([7-9]\\d{1})$', replacement='19\\1')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   cultivo     162 non-null    object 
#   1   campania    162 non-null    object 
#   2   rindes_avg  161 non-null    float64
#  
#  |    | cultivo   |   campania |   rindes_avg |
#  |---:|:----------|-----------:|-------------:|
#  |  0 | Maíz      |       1970 |       2330.5 |
#  
#  ------------------------------
#  
#  regreplace_text(col='campania', pattern='^([0-2]\\d{1})$', replacement='20\\1')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   cultivo     162 non-null    object 
#   1   campania    162 non-null    object 
#   2   rindes_avg  161 non-null    float64
#  
#  |    | cultivo   |   campania |   rindes_avg |
#  |---:|:----------|-----------:|-------------:|
#  |  0 | Maíz      |       1970 |       2330.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cultivo': 'categoria', 'rindes_avg': 'valor', 'campania': 'anio'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    object 
#   2   valor      161 non-null    float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Maíz        |   1970 |  2330.5 |
#  
#  ------------------------------
#  