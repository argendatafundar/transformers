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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc_abrev': 'nivel2', 'particip_vab': 'valor'}),
	drop_col(col=['letra', 'id_tipo_sector', 'anio'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='nivel2', curr_value='Petróleo y minería', new_value='Petróleo y \nminería'),
	replace_value(col='nivel2', curr_value='Industria manufacturera', new_value='Industria \nmanufacturera'),
	replace_value(col='nivel2', curr_value='Electricidad, gas y agua', new_value='Electricidad, \ngas y agua'),
	replace_value(col='nivel2', curr_value='Hotelería y restaurantes', new_value='Hotelería y \nrestaurantes'),
	replace_value(col='nivel2', curr_value='Serv. inmobiliarios y profesionales', new_value='Servicios \ninmobiliarios y \nprofesionales'),
	replace_value(col='nivel2', curr_value='Adm. pública y defensa', new_value='Administración \npública \ny defensa'),
	replace_value(col='nivel2', curr_value='Serv. comunitarios, sociales y personales', new_value='Servicios \ncomunitarios, \nsociales y \npersonales'),
	replace_value(col='nivel2', curr_value='Servicio doméstico', new_value='Servicio \ndoméstico'),
	replace_value(col='nivel2', curr_value='Transporte y comunicaciones', new_value='Transporte y \ncomunicaciones')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 320 entries, 0 to 319
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              320 non-null    int64  
#   1   letra             320 non-null    object 
#   2   letra_desc_abrev  320 non-null    object 
#   3   id_tipo_sector    320 non-null    int64  
#   4   tipo_sector       320 non-null    object 
#   5   particip_vab      320 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   id_tipo_sector | tipo_sector   |   particip_vab |
#  |---:|-------:|:--------|:-------------------|-----------------:|:--------------|---------------:|
#  |  0 |   2004 | A       | Agro               |                1 | Bienes        |      0.0948656 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 16 entries, 304 to 319
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              16 non-null     int64  
#   1   letra             16 non-null     object 
#   2   letra_desc_abrev  16 non-null     object 
#   3   id_tipo_sector    16 non-null     int64  
#   4   tipo_sector       16 non-null     object 
#   5   particip_vab      16 non-null     float64
#  
#  |     |   anio | letra   | letra_desc_abrev   |   id_tipo_sector | tipo_sector   |   particip_vab |
#  |----:|-------:|:--------|:-------------------|-----------------:|:--------------|---------------:|
#  | 304 |   2023 | A       | Agro               |                1 | Bienes        |      0.0694929 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc_abrev': 'nivel2', 'particip_vab': 'valor'})
#  Index: 16 entries, 304 to 319
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            16 non-null     int64  
#   1   letra           16 non-null     object 
#   2   nivel2          16 non-null     object 
#   3   id_tipo_sector  16 non-null     int64  
#   4   nivel1          16 non-null     object 
#   5   valor           16 non-null     float64
#  
#  |     |   anio | letra   | nivel2   |   id_tipo_sector | nivel1   |     valor |
#  |----:|-------:|:--------|:---------|-----------------:|:---------|----------:|
#  | 304 |   2023 | A       | Agro     |                1 | Bienes   | 0.0694929 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'id_tipo_sector', 'anio'], axis=1)
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Petróleo y minería', new_value='Petróleo y \nminería')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Industria manufacturera', new_value='Industria \nmanufacturera')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Electricidad, gas y agua', new_value='Electricidad, \ngas y agua')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Hotelería y restaurantes', new_value='Hotelería y \nrestaurantes')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Serv. inmobiliarios y profesionales', new_value='Servicios \ninmobiliarios y \nprofesionales')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Adm. pública y defensa', new_value='Administración \npública \ny defensa')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Serv. comunitarios, sociales y personales', new_value='Servicios \ncomunitarios, \nsociales y \npersonales')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Servicio doméstico', new_value='Servicio \ndoméstico')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Transporte y comunicaciones', new_value='Transporte y \ncomunicaciones')
#  Index: 16 entries, 304 to 319
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  16 non-null     object 
#   1   nivel1  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |     | nivel2   | nivel1   |   valor |
#  |----:|:---------|:---------|--------:|
#  | 304 | Agro     | Bienes   | 6.94929 |
#  
#  ------------------------------
#  