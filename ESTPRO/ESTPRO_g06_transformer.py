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
def round_col(df:DataFrame, col:str, decimals:int):
    df[col] = df[col].round(decimals)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc_abrev': 'nivel2', 'share_sectorial': 'valor'}),
	drop_col(col=['letra', 'id_tipo_sector'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100),
	round_col(col='valor', decimals=1),
	replace_value(col='nivel2', curr_value='Serv. comunitarios, sociales y personales', new_value='Salud y serv. personales')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 266 entries, 0 to 265
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              266 non-null    int64  
#   1   letra             266 non-null    object 
#   2   letra_desc_abrev  266 non-null    object 
#   3   share_sectorial   266 non-null    float64
#   4   id_tipo_sector    266 non-null    int64  
#   5   tipo_sector       266 non-null    object 
#  
#  |    |   anio | letra   | letra_desc_abrev   |   share_sectorial |   id_tipo_sector | tipo_sector   |
#  |---:|-------:|:--------|:-------------------|------------------:|-----------------:|:--------------|
#  |  0 |   2004 | AB      | Agro y pesca       |         0.0836072 |                1 | Bienes        |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 14 entries, 252 to 265
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              14 non-null     int64  
#   1   letra             14 non-null     object 
#   2   letra_desc_abrev  14 non-null     object 
#   3   share_sectorial   14 non-null     float64
#   4   id_tipo_sector    14 non-null     int64  
#   5   tipo_sector       14 non-null     object 
#  
#  |     |   anio | letra   | letra_desc_abrev   |   share_sectorial |   id_tipo_sector | tipo_sector   |
#  |----:|-------:|:--------|:-------------------|------------------:|-----------------:|:--------------|
#  | 252 |   2022 | AB      | Agro y pesca       |         0.0637079 |                1 | Bienes        |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc_abrev': 'nivel2', 'share_sectorial': 'valor'})
#  Index: 14 entries, 252 to 265
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            14 non-null     int64  
#   1   letra           14 non-null     object 
#   2   nivel2          14 non-null     object 
#   3   valor           14 non-null     float64
#   4   id_tipo_sector  14 non-null     int64  
#   5   nivel1          14 non-null     object 
#  
#  |     |   anio | letra   | nivel2       |     valor |   id_tipo_sector | nivel1   |
#  |----:|-------:|:--------|:-------------|----------:|-----------------:|:---------|
#  | 252 |   2022 | AB      | Agro y pesca | 0.0637079 |                1 | Bienes   |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'id_tipo_sector'], axis=1)
#  Index: 14 entries, 252 to 265
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#   3   nivel1  14 non-null     object 
#  
#  |     |   anio | nivel2       |   valor | nivel1   |
#  |----:|-------:|:-------------|--------:|:---------|
#  | 252 |   2022 | Agro y pesca |     6.4 | Bienes   |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 14 entries, 252 to 265
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#   3   nivel1  14 non-null     object 
#  
#  |     |   anio | nivel2       |   valor | nivel1   |
#  |----:|-------:|:-------------|--------:|:---------|
#  | 252 |   2022 | Agro y pesca |     6.4 | Bienes   |
#  
#  ------------------------------
#  
#  round_col(col='valor', decimals=1)
#  Index: 14 entries, 252 to 265
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#   3   nivel1  14 non-null     object 
#  
#  |     |   anio | nivel2       |   valor | nivel1   |
#  |----:|-------:|:-------------|--------:|:---------|
#  | 252 |   2022 | Agro y pesca |     6.4 | Bienes   |
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', curr_value='Serv. comunitarios, sociales y personales', new_value='Salud y serv. personales')
#  Index: 14 entries, 252 to 265
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#   3   nivel1  14 non-null     object 
#  
#  |     |   anio | nivel2       |   valor | nivel1   |
#  |----:|-------:|:-------------|--------:|:---------|
#  | 252 |   2022 | Agro y pesca |     6.4 | Bienes   |
#  
#  ------------------------------
#  