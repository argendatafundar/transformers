from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
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
multiplicar_por_escalar(col='tamanio_mercado', k=1e-06),
	wide_to_long(primary_keys=['codigo_pais', 'pais', 'mineral'], value_name='valor', var_name='indicador'),
	drop_col(col='pais', axis=1),
	rename_cols(map={'codigo_pais': 'grupo', 'mineral': 'categoria'}),
	replace_value(col='grupo', curr_value='CHN', new_value='China'),
	replace_value(col='grupo', curr_value='CHL', new_value='Chile'),
	replace_value(col='grupo', curr_value='AUS', new_value='Australia'),
	replace_value(col='grupo', curr_value='MEX', new_value='México'),
	replace_value(col='grupo', curr_value='ZAF', new_value='Sudáfrica'),
	replace_value(col='grupo', curr_value='COD', new_value='Rep. Dem. Congo')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   codigo_pais      13 non-null     object 
#   1   mineral          13 non-null     object 
#   2   pais             13 non-null     object 
#   3   share            13 non-null     float64
#   4   tamanio_mercado  13 non-null     float64
#  
#  |    | codigo_pais   | mineral           | pais      |    share |   tamanio_mercado |
#  |---:|:--------------|:------------------|:----------|---------:|------------------:|
#  |  0 | AUS           | Mineral de Hierro | Australia | 0.338462 |        3.1383e-07 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tamanio_mercado', k=1e-06)
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   codigo_pais      13 non-null     object 
#   1   mineral          13 non-null     object 
#   2   pais             13 non-null     object 
#   3   share            13 non-null     float64
#   4   tamanio_mercado  13 non-null     float64
#  
#  |    | codigo_pais   | mineral           | pais      |    share |   tamanio_mercado |
#  |---:|:--------------|:------------------|:----------|---------:|------------------:|
#  |  0 | AUS           | Mineral de Hierro | Australia | 0.338462 |        3.1383e-13 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['codigo_pais', 'pais', 'mineral'], value_name='valor', var_name='indicador')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   codigo_pais  26 non-null     object 
#   1   pais         26 non-null     object 
#   2   mineral      26 non-null     object 
#   3   indicador    26 non-null     object 
#   4   valor        26 non-null     float64
#  
#  |    | codigo_pais   | pais      | mineral           | indicador   |    valor |
#  |---:|:--------------|:----------|:------------------|:------------|---------:|
#  |  0 | AUS           | Australia | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   codigo_pais  26 non-null     object 
#   1   mineral      26 non-null     object 
#   2   indicador    26 non-null     object 
#   3   valor        26 non-null     float64
#  
#  |    | codigo_pais   | mineral           | indicador   |    valor |
#  |---:|:--------------|:------------------|:------------|---------:|
#  |  0 | AUS           | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  rename_cols(map={'codigo_pais': 'grupo', 'mineral': 'categoria'})
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo   | categoria         | indicador   |    valor |
#  |---:|:--------|:------------------|:------------|---------:|
#  |  0 | AUS     | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='CHN', new_value='China')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo   | categoria         | indicador   |    valor |
#  |---:|:--------|:------------------|:------------|---------:|
#  |  0 | AUS     | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='CHL', new_value='Chile')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo   | categoria         | indicador   |    valor |
#  |---:|:--------|:------------------|:------------|---------:|
#  |  0 | AUS     | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='AUS', new_value='Australia')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo     | categoria         | indicador   |    valor |
#  |---:|:----------|:------------------|:------------|---------:|
#  |  0 | Australia | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='MEX', new_value='México')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo     | categoria         | indicador   |    valor |
#  |---:|:----------|:------------------|:------------|---------:|
#  |  0 | Australia | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='ZAF', new_value='Sudáfrica')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo     | categoria         | indicador   |    valor |
#  |---:|:----------|:------------------|:------------|---------:|
#  |  0 | Australia | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  
#  replace_value(col='grupo', curr_value='COD', new_value='Rep. Dem. Congo')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      26 non-null     object 
#   1   categoria  26 non-null     object 
#   2   indicador  26 non-null     object 
#   3   valor      26 non-null     float64
#  
#  |    | grupo     | categoria         | indicador   |    valor |
#  |---:|:----------|:------------------|:------------|---------:|
#  |  0 | Australia | Mineral de Hierro | share       | 0.338462 |
#  
#  ------------------------------
#  