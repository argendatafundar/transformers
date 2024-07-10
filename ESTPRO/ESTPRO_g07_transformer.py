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
	rename_cols(map={'tipo_sector': 'grupo', 'letra_desc_abrev': 'categoria'}),
	drop_col(col=['anio', 'letra', 'id_tipo_sector'], axis=1),
	wide_to_long(primary_keys=['grupo', 'categoria'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='share_sectorial', new_value='Share sectorial'),
	replace_value(col='indicador', curr_value='share_vab', new_value='Share VAB')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 266 entries, 0 to 265
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              266 non-null    int64  
#   1   letra             266 non-null    object 
#   2   letra_desc_abrev  266 non-null    object 
#   3   share_sectorial   266 non-null    float64
#   4   id_tipo_sector    266 non-null    int64  
#   5   tipo_sector       266 non-null    object 
#   6   share_vab         266 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   share_sectorial |   id_tipo_sector | tipo_sector   |   share_vab |
#  |---:|-------:|:--------|:-------------------|------------------:|-----------------:|:--------------|------------:|
#  |  0 |   2004 | AB      | Agro y pesca       |         0.0836072 |                1 | Bienes        |   0.0983632 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 14 entries, 252 to 265
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              14 non-null     int64  
#   1   letra             14 non-null     object 
#   2   letra_desc_abrev  14 non-null     object 
#   3   share_sectorial   14 non-null     float64
#   4   id_tipo_sector    14 non-null     int64  
#   5   tipo_sector       14 non-null     object 
#   6   share_vab         14 non-null     float64
#  
#  |     |   anio | letra   | letra_desc_abrev   |   share_sectorial |   id_tipo_sector | tipo_sector   |   share_vab |
#  |----:|-------:|:--------|:-------------------|------------------:|-----------------:|:--------------|------------:|
#  | 252 |   2022 | AB      | Agro y pesca       |         0.0637079 |                1 | Bienes        |   0.0791809 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_sector': 'grupo', 'letra_desc_abrev': 'categoria'})
#  Index: 14 entries, 252 to 265
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             14 non-null     int64  
#   1   letra            14 non-null     object 
#   2   categoria        14 non-null     object 
#   3   share_sectorial  14 non-null     float64
#   4   id_tipo_sector   14 non-null     int64  
#   5   grupo            14 non-null     object 
#   6   share_vab        14 non-null     float64
#  
#  |     |   anio | letra   | categoria    |   share_sectorial |   id_tipo_sector | grupo   |   share_vab |
#  |----:|-------:|:--------|:-------------|------------------:|-----------------:|:--------|------------:|
#  | 252 |   2022 | AB      | Agro y pesca |         0.0637079 |                1 | Bienes  |   0.0791809 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'letra', 'id_tipo_sector'], axis=1)
#  Index: 14 entries, 252 to 265
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   categoria        14 non-null     object 
#   1   share_sectorial  14 non-null     float64
#   2   grupo            14 non-null     object 
#   3   share_vab        14 non-null     float64
#  
#  |     | categoria    |   share_sectorial | grupo   |   share_vab |
#  |----:|:-------------|------------------:|:--------|------------:|
#  | 252 | Agro y pesca |         0.0637079 | Bienes  |   0.0791809 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['grupo', 'categoria'], value_name='valor', var_name='indicador')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      28 non-null     object 
#   1   categoria  28 non-null     object 
#   2   indicador  28 non-null     object 
#   3   valor      28 non-null     float64
#  
#  |    | grupo   | categoria    | indicador       |     valor |
#  |---:|:--------|:-------------|:----------------|----------:|
#  |  0 | Bienes  | Agro y pesca | share_sectorial | 0.0637079 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='share_sectorial', new_value='Share sectorial')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      28 non-null     object 
#   1   categoria  28 non-null     object 
#   2   indicador  28 non-null     object 
#   3   valor      28 non-null     float64
#  
#  |    | grupo   | categoria    | indicador       |     valor |
#  |---:|:--------|:-------------|:----------------|----------:|
#  |  0 | Bienes  | Agro y pesca | Share sectorial | 0.0637079 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='share_vab', new_value='Share VAB')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      28 non-null     object 
#   1   categoria  28 non-null     object 
#   2   indicador  28 non-null     object 
#   3   valor      28 non-null     float64
#  
#  |    | grupo   | categoria    | indicador       |     valor |
#  |---:|:--------|:-------------|:----------------|----------:|
#  |  0 | Bienes  | Agro y pesca | Share sectorial | 0.0637079 |
#  
#  ------------------------------
#  