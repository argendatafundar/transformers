from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

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
drop_col(col=['region_pbg', 'pib_pc_ultimo_anio', 'provincia_nombre'], axis=1),
	rename_cols(map={'provincia_id': 'geocodigo'}),
	multiplicar_por_escalar(col='var_pib_pc_1895_ultimo_anio', k=100),
	wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita provincial en 1895 (en pesos de 2004)'),
	replace_value(col='indicador', curr_value='var_pib_pc_1895_ultimo_anio', new_value='Variación del PIB per cápita provincial entre 1895 y 2022')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 6 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   provincia_id                 24 non-null     object 
#   1   provincia_nombre             24 non-null     object 
#   2   region_pbg                   24 non-null     object 
#   3   pib_pc_1895                  24 non-null     float64
#   4   pib_pc_ultimo_anio           24 non-null     float64
#   5   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | provincia_id   | provincia_nombre   | region_pbg      |   pib_pc_1895 |   pib_pc_ultimo_anio |   var_pib_pc_1895_ultimo_anio |
#  |---:|:---------------|:-------------------|:----------------|--------------:|---------------------:|------------------------------:|
#  |  0 | AR-B           | Buenos Aires       | Pampeana y AMBA |       4413.34 |              10992.4 |                       1.49071 |
#  
#  ------------------------------
#  
#  drop_col(col=['region_pbg', 'pib_pc_ultimo_anio', 'provincia_nombre'], axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   provincia_id                 24 non-null     object 
#   1   pib_pc_1895                  24 non-null     float64
#   2   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | provincia_id   |   pib_pc_1895 |   var_pib_pc_1895_ultimo_anio |
#  |---:|:---------------|--------------:|------------------------------:|
#  |  0 | AR-B           |       4413.34 |                       1.49071 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigo                    24 non-null     object 
#   1   pib_pc_1895                  24 non-null     float64
#   2   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | geocodigo   |   pib_pc_1895 |   var_pib_pc_1895_ultimo_anio |
#  |---:|:------------|--------------:|------------------------------:|
#  |  0 | AR-B        |       4413.34 |                       149.071 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='var_pib_pc_1895_ultimo_anio', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigo                    24 non-null     object 
#   1   pib_pc_1895                  24 non-null     float64
#   2   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | geocodigo   |   pib_pc_1895 |   var_pib_pc_1895_ultimo_anio |
#  |---:|:------------|--------------:|------------------------------:|
#  |  0 | AR-B        |       4413.34 |                       149.071 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-B        | pib_pc_1895 | 4413.34 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita provincial en 1895 (en pesos de 2004)')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                                            |   valor |
#  |---:|:------------|:-----------------------------------------------------|--------:|
#  |  0 | AR-B        | PIB per cápita provincial en 1895 (en pesos de 2004) | 4413.34 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='var_pib_pc_1895_ultimo_anio', new_value='Variación del PIB per cápita provincial entre 1895 y 2022')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                                            |   valor |
#  |---:|:------------|:-----------------------------------------------------|--------:|
#  |  0 | AR-B        | PIB per cápita provincial en 1895 (en pesos de 2004) | 4413.34 |
#  
#  ------------------------------
#  