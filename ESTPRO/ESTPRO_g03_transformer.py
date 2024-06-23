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
	rename_cols(map={'iso3': 'geocodigo', 'particip_servicios_pib': 'valor'}),
	drop_col(col=['iso3_desc_fundar', 'es_agregacion'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 11660 entries, 0 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    11660 non-null  int64  
#   1   iso3                    11660 non-null  object 
#   2   iso3_desc_fundar        11660 non-null  object 
#   3   es_agregacion           11660 non-null  int64  
#   4   particip_servicios_pib  10467 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |---:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  |  0 |   1970 | AFG    | Afganistán         |               0 |                 0.252995 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='KOS', new_value='XKX')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SD1', new_value='SDN')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SV1', new_value='SUV')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ1', new_value='TZA')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ2', new_value='ZPM')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='YUG', new_value='SER')
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    220 non-null    int64  
#   1   iso3                    220 non-null    object 
#   2   iso3_desc_fundar        220 non-null    object 
#   3   es_agregacion           220 non-null    int64  
#   4   particip_servicios_pib  206 non-null    float64
#  
#  |       |   anio | iso3   | iso3_desc_fundar   |   es_agregacion |   particip_servicios_pib |
#  |------:|-------:|:-------|:-------------------|----------------:|-------------------------:|
#  | 11440 |   2022 | AFG    | Afganistán         |               0 |                 0.475201 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'particip_servicios_pib': 'valor'})
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              220 non-null    int64  
#   1   geocodigo         220 non-null    object 
#   2   iso3_desc_fundar  220 non-null    object 
#   3   es_agregacion     220 non-null    int64  
#   4   valor             206 non-null    float64
#  
#  |       |   anio | geocodigo   | iso3_desc_fundar   |   es_agregacion |    valor |
#  |------:|-------:|:------------|:-------------------|----------------:|---------:|
#  | 11440 |   2022 | AFG         | Afganistán         |               0 | 0.475201 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3_desc_fundar', 'es_agregacion'], axis=1)
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       220 non-null    int64  
#   1   geocodigo  220 non-null    object 
#   2   valor      206 non-null    float64
#  
#  |       |   anio | geocodigo   |   valor |
#  |------:|-------:|:------------|--------:|
#  | 11440 |   2022 | AFG         | 47.5201 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 220 entries, 11440 to 11659
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       220 non-null    int64  
#   1   geocodigo  220 non-null    object 
#   2   valor      206 non-null    float64
#  
#  |       |   anio | geocodigo   |   valor |
#  |------:|-------:|:------------|--------:|
#  | 11440 |   2022 | AFG         | 47.5201 |
#  
#  ------------------------------
#  