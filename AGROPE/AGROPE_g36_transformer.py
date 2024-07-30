from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='tipo_cadena', curr_value='Maiz', new_value='Maíz'),
	replace_value(col='tipo_cadena', curr_value='Bovino', new_value='Bovina'),
	replace_value(col='tipo_cadena', curr_value='Lacteo', new_value='Lácteo'),
	replace_value(col='tipo_cadena', curr_value='Avicola', new_value='Avícola'),
	replace_value(col='tipo_cadena', curr_value='Porcinos', new_value='Porcina'),
	replace_value(col='tipo_cadena', curr_value='Citrico', new_value='Cítrico'),
	replace_value(col='tipo_cadena', curr_value='Caprino', new_value='Caprina'),
	replace_value(col='tipo_cadena', curr_value='Ovinos', new_value='Ovina'),
	rename_cols(map={'tipo_cadena': 'categoria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |    0.86 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Maiz', new_value='Maíz')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Bovino', new_value='Bovina')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Lacteo', new_value='Lácteo')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Avicola', new_value='Avícola')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Porcinos', new_value='Porcina')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Citrico', new_value='Cítrico')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Caprino', new_value='Caprina')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_cadena', curr_value='Ovinos', new_value='Ovina')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |      86 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_cadena': 'categoria'})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Miel        |      86 |
#  
#  ------------------------------
#  