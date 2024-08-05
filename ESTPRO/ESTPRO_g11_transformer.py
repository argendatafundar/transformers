from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
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
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
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
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'letra_desc_abrev': 'categoria'}),
	drop_col(col=['letra'], axis=1),
	wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador'),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='porc_mujeres', new_value='Mujeres'),
	replace_value(col='indicador', curr_value='porc_varones', new_value='Varones'),
	ordenar_dos_columnas(col1='indicador', order1=['Mujeres', 'Varones'], col2='categoria', order2=['Construcción', 'Petróleo y minería', 'Agro y pesca', 'Transporte y comunicaciones', 'Electricidad, gas y agua', 'Industria manufacturera', 'Serv. inmobiliarios y profesionales', 'Comercio', 'Total', 'Hotelería y restaurantes', 'Finanzas', 'Serv. comunitarios, sociales y personales', 'Adm. pública y defensa', 'Salud', 'Enseñanza', 'Servicio doméstico'])
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
#  latest_year(by='anio')
#  Index: 16 entries, 6 to 111
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             16 non-null     object 
#   1   letra_desc_abrev  16 non-null     object 
#   2   porc_mujeres      16 non-null     float64
#   3   porc_varones      16 non-null     float64
#  
#  |    | letra   | letra_desc_abrev   |   porc_mujeres |   porc_varones |
#  |---:|:--------|:-------------------|---------------:|---------------:|
#  |  6 | AB      | Agro y pesca       |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria'})
#  Index: 16 entries, 6 to 111
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   letra         16 non-null     object 
#   1   categoria     16 non-null     object 
#   2   porc_mujeres  16 non-null     float64
#   3   porc_varones  16 non-null     float64
#  
#  |    | letra   | categoria    |   porc_mujeres |   porc_varones |
#  |---:|:--------|:-------------|---------------:|---------------:|
#  |  6 | AB      | Agro y pesca |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra'], axis=1)
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
#  multiplicar_por_escalar(col='valor', k=100)
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
#  replace_value(col='indicador', curr_value='porc_mujeres', new_value='Mujeres')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador   |   valor |
#  |---:|:-------------|:------------|--------:|
#  |  0 | Agro y pesca | Mujeres     | 14.0203 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porc_varones', new_value='Varones')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  32 non-null     category
#   1   indicador  32 non-null     category
#   2   valor      32 non-null     float64 
#  
#  |    | categoria    | indicador   |   valor |
#  |---:|:-------------|:------------|--------:|
#  |  0 | Agro y pesca | Mujeres     | 14.0203 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='indicador', order1=['Mujeres', 'Varones'], col2='categoria', order2=['Construcción', 'Petróleo y minería', 'Agro y pesca', 'Transporte y comunicaciones', 'Electricidad, gas y agua', 'Industria manufacturera', 'Serv. inmobiliarios y profesionales', 'Comercio', 'Total', 'Hotelería y restaurantes', 'Finanzas', 'Serv. comunitarios, sociales y personales', 'Adm. pública y defensa', 'Salud', 'Enseñanza', 'Servicio doméstico'])
#  Index: 32 entries, 4 to 30
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  32 non-null     category
#   1   indicador  32 non-null     category
#   2   valor      32 non-null     float64 
#  
#  |    | categoria    | indicador   |   valor |
#  |---:|:-------------|:------------|--------:|
#  |  4 | Construcción | Mujeres     | 4.93933 |
#  
#  ------------------------------
#  