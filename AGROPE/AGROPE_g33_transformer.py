from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'cadena': 'categoria'}),
	mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='cadena', curr_value='Maiz', new_value='Maíz'),
	replace_value(col='cadena', curr_value='Bovino', new_value='Bovina'),
	replace_value(col='cadena', curr_value='Lacteo', new_value='Lácteo'),
	replace_value(col='cadena', curr_value='Avicola', new_value='Avícola'),
	replace_value(col='cadena', curr_value='Porcinos', new_value='Porcina'),
	replace_value(col='cadena', curr_value='Citrico', new_value='Cítrico'),
	replace_value(col='cadena', curr_value='Caprino', new_value='Caprina')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   cadena  31 non-null     object 
#   1   valor   31 non-null     float64
#  
#  |    | cadena   |   valor |
#  |---:|:---------|--------:|
#  |  0 | Soja     |   0.236 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cadena': 'categoria'})
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Maiz', new_value='Maíz')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Bovino', new_value='Bovina')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Lacteo', new_value='Lácteo')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Avicola', new_value='Avícola')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Porcinos', new_value='Porcina')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Citrico', new_value='Cítrico')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  
#  replace_value(col='cadena', curr_value='Caprino', new_value='Caprina')
#  RangeIndex: 31 entries, 0 to 30
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  31 non-null     object 
#   1   valor      31 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Soja        |    23.6 |
#  
#  ------------------------------
#  