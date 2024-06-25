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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	rename_cols(map={'letra_desc_abrev': 'categoria'}),
	drop_col(col=['letra', 'anio'], axis=1),
	wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador'),
	mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='porc_mujeres', new_value='Porcentaje de mujeres'),
	replace_value(col='indicador', curr_value='porc_varones', new_value='Porcentaje de varones')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              112 non-null    int64  
#   1   letra             112 non-null    object 
#   2   letra_desc_abrev  112 non-null    object 
#   3   porc_mujeres      112 non-null    float64
#   4   porc_varones      112 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   porc_mujeres |   porc_varones |
#  |---:|-------:|:--------|:-------------------|---------------:|---------------:|
#  |  0 |   2016 | AB      | Agro y pesca       |       0.142606 |       0.857394 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 16 entries, 6 to 111
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              16 non-null     int64  
#   1   letra             16 non-null     object 
#   2   letra_desc_abrev  16 non-null     object 
#   3   porc_mujeres      16 non-null     float64
#   4   porc_varones      16 non-null     float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   porc_mujeres |   porc_varones |
#  |---:|-------:|:--------|:-------------------|---------------:|---------------:|
#  |  6 |   2022 | AB      | Agro y pesca       |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria'})
#  Index: 16 entries, 6 to 111
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          16 non-null     int64  
#   1   letra         16 non-null     object 
#   2   categoria     16 non-null     object 
#   3   porc_mujeres  16 non-null     float64
#   4   porc_varones  16 non-null     float64
#  
#  |    |   anio | letra   | categoria    |   porc_mujeres |   porc_varones |
#  |---:|-------:|:--------|:-------------|---------------:|---------------:|
#  |  6 |   2022 | AB      | Agro y pesca |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'anio'], axis=1)
#  Index: 16 entries, 6 to 111
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     16 non-null     object 
#   1   porc_mujeres  16 non-null     float64
#   2   porc_varones  16 non-null     float64
#  
#  |    | categoria    |   porc_mujeres |   porc_varones |
#  |---:|:-------------|---------------:|---------------:|
#  |  6 | Agro y pesca |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador    |   valor |
#  |---:|:-------------|:-------------|--------:|
#  |  0 | Agro y pesca | porc_mujeres | 14.0203 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador    |   valor |
#  |---:|:-------------|:-------------|--------:|
#  |  0 | Agro y pesca | porc_mujeres | 14.0203 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porc_mujeres', new_value='Porcentaje de mujeres')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador             |   valor |
#  |---:|:-------------|:----------------------|--------:|
#  |  0 | Agro y pesca | Porcentaje de mujeres | 14.0203 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porc_varones', new_value='Porcentaje de varones')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador             |   valor |
#  |---:|:-------------|:----------------------|--------:|
#  |  0 | Agro y pesca | Porcentaje de mujeres | 14.0203 |
#  
#  ------------------------------
#  