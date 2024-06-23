from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	replace_value(col='iso3', curr_value='KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='SD1', new_value='SDN'),
	replace_value(col='iso3', curr_value='SV1', new_value='SUV'),
	replace_value(col='iso3', curr_value='TZ1', new_value='TZA'),
	replace_value(col='iso3', curr_value='TZ2', new_value='ZPM'),
	replace_value(col='iso3', curr_value='YUG', new_value='SER'),
	rename_cols(map={'iso3': 'geocodigo', 'particip_va_pib': 'valor', 'gran_sector': 'indicador'}),
	drop_col(col=['iso3_desc_fundar', 'es_agregacion', 'letra', 'id_tipo_sector', 'tipo_sector'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81461 entries, 0 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              81461 non-null  int64  
#   1   iso3              81461 non-null  object 
#   2   iso3_desc_fundar  81461 non-null  object 
#   3   es_agregacion     81461 non-null  int64  
#   4   letra             81461 non-null  object 
#   5   gran_sector       81461 non-null  object 
#   6   id_tipo_sector    81461 non-null  int64  
#   7   tipo_sector       81461 non-null  object 
#   8   particip_va_pib   73145 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |---:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  |  0 |   1970 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.502422 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='KOS', new_value='XKX')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SD1', new_value='SDN')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SV1', new_value='SUV')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ1', new_value='TZA')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ2', new_value='ZPM')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='YUG', new_value='SER')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   iso3              1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   gran_sector       1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   particip_va_pib   1439 non-null   float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 |   2022 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'particip_va_pib': 'valor', 'gran_sector': 'indicador'})
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 9 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              1537 non-null   int64  
#   1   geocodigo         1537 non-null   object 
#   2   iso3_desc_fundar  1537 non-null   object 
#   3   es_agregacion     1537 non-null   int64  
#   4   letra             1537 non-null   object 
#   5   indicador         1537 non-null   object 
#   6   id_tipo_sector    1537 non-null   int64  
#   7   tipo_sector       1537 non-null   object 
#   8   valor             1439 non-null   float64
#  
#  |       |   anio | geocodigo   | iso3_desc_fundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |    valor |
#  |------:|-------:|:------------|:-------------------|----------------:|:--------|:-------------|-----------------:|:--------------|---------:|
#  | 79924 |   2022 | AFG         | Afganistán         |               0 | AB      | Agro y pesca |                1 | Bienes        | 0.355494 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3_desc_fundar', 'es_agregacion', 'letra', 'id_tipo_sector', 'tipo_sector'], axis=1)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1537 non-null   int64  
#   1   geocodigo  1537 non-null   object 
#   2   indicador  1537 non-null   object 
#   3   valor      1439 non-null   float64
#  
#  |       |   anio | geocodigo   | indicador    |   valor |
#  |------:|-------:|:------------|:-------------|--------:|
#  | 79924 |   2022 | AFG         | Agro y pesca | 35.5494 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1537 non-null   int64  
#   1   geocodigo  1537 non-null   object 
#   2   indicador  1537 non-null   object 
#   3   valor      1439 non-null   float64
#  
#  |       |   anio | geocodigo   | indicador    |   valor |
#  |------:|-------:|:------------|:-------------|--------:|
#  | 79924 |   2022 | AFG         | Agro y pesca | 35.5494 |
#  
#  ------------------------------
#  