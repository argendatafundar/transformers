from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

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
query(condition='anio == anio.max()'),
	rename_cols(map={'letra_desc_abrev': 'categoria'}),
	drop_col(col=['anio', 'letra'], axis=1),
	wide_to_long(primary_keys='categoria', value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='indice_salario', new_value='Índice salario'),
	replace_value(col='indicador', curr_value='escala', new_value='Escala')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 364 entries, 0 to 363
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             364 non-null    object 
#   1   anio              364 non-null    int64  
#   2   letra_desc_abrev  364 non-null    object 
#   3   indice_salario    364 non-null    float64
#   4   escala            364 non-null    float64
#  
#  |    | letra   |   anio | letra_desc_abrev   |   indice_salario |   escala |
#  |---:|:--------|-------:|:-------------------|-----------------:|---------:|
#  |  0 | A       |   1996 | Agro               |          47.9175 |  4.12243 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 14 entries, 25 to 363
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             14 non-null     object 
#   1   anio              14 non-null     int64  
#   2   letra_desc_abrev  14 non-null     object 
#   3   indice_salario    14 non-null     float64
#   4   escala            14 non-null     float64
#  
#  |    | letra   |   anio | letra_desc_abrev   |   indice_salario |   escala |
#  |---:|:--------|-------:|:-------------------|-----------------:|---------:|
#  | 25 | A       |   2021 | Agro               |          61.3502 |  5.91369 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria'})
#  Index: 14 entries, 25 to 363
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   letra           14 non-null     object 
#   1   anio            14 non-null     int64  
#   2   categoria       14 non-null     object 
#   3   indice_salario  14 non-null     float64
#   4   escala          14 non-null     float64
#  
#  |    | letra   |   anio | categoria   |   indice_salario |   escala |
#  |---:|:--------|-------:|:------------|-----------------:|---------:|
#  | 25 | A       |   2021 | Agro        |          61.3502 |  5.91369 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'letra'], axis=1)
#  Index: 14 entries, 25 to 363
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       14 non-null     object 
#   1   indice_salario  14 non-null     float64
#   2   escala          14 non-null     float64
#  
#  |    | categoria   |   indice_salario |   escala |
#  |---:|:------------|-----------------:|---------:|
#  | 25 | Agro        |          61.3502 |  5.91369 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys='categoria', value_name='valor', var_name='indicador')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  28 non-null     object 
#   1   indicador  28 non-null     object 
#   2   valor      28 non-null     float64
#  
#  |    | categoria   | indicador      |   valor |
#  |---:|:------------|:---------------|--------:|
#  |  0 | Agro        | indice_salario | 61.3502 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='indice_salario', new_value='Índice salario')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  28 non-null     object 
#   1   indicador  28 non-null     object 
#   2   valor      28 non-null     float64
#  
#  |    | categoria   | indicador      |   valor |
#  |---:|:------------|:---------------|--------:|
#  |  0 | Agro        | Índice salario | 61.3502 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='escala', new_value='Escala')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  28 non-null     object 
#   1   indicador  28 non-null     object 
#   2   valor      28 non-null     float64
#  
#  |    | categoria   | indicador      |   valor |
#  |---:|:------------|:---------------|--------:|
#  |  0 | Agro        | Índice salario | 61.3502 |
#  
#  ------------------------------
#  