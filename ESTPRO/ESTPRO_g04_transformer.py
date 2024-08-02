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
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
	replace_value(col='iso3', curr_value='KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='SD1', new_value='SDN'),
	replace_value(col='iso3', curr_value='SV1', new_value='SUV'),
	replace_value(col='iso3', curr_value='TZ1', new_value='TZA'),
	replace_value(col='iso3', curr_value='TZ2', new_value='ZPM'),
	replace_value(col='iso3', curr_value='YUG', new_value='SER'),
	rename_cols(map={'iso3': 'geocodigo', 'particip_va_pib': 'valor', 'gran_sector': 'indicador'}),
	drop_col(col=['iso3_desc_fundar', 'es_agregacion', 'letra', 'id_tipo_sector', 'tipo_sector'], axis=1),
	multiplicar_por_escalar(col='valor', k=100),
	ordenar_dos_columnas(col1='geocodigo', order1=['BMU', 'HKG', 'VGB', 'SXM', 'CYM', 'BHS', 'MAC', 'LUX', 'MCO', 'ABW', 'AND', 'AIA', 'DJI', 'LCA', 'CUW', 'MLT', 'PYF', 'CYP', 'PLW', 'TCA', 'MSR', 'BRB', 'SYC', 'MDV', 'LBN', 'GBR', 'USA', 'CUB', 'FRA', 'ISR', 'BEL', 'VCT', 'GRC', 'NLD', 'PRT', 'MNE', 'DNK', 'GRD', 'WSM', 'KNA', 'MUS', 'NCL', 'ATG', 'ESP', 'SGP', 'CHE', 'STP', 'CRI', 'CAN', 'HRV', 'BLZ', 'NZL', 'ITA', 'ISL', 'URY', 'SWE', 'PSE', 'FSM', 'PAN', 'EST', 'JPN', 'LVA', 'ZAF', 'AUT', 'DEU', 'UKR', 'GEO', 'AUS', 'BRA', 'FIN', 'JOR', 'JAM', 'MDA', 'LTU', 'HUN', 'BGR', 'SLV', 'FJI', 'DMA', 'GTM', 'SVN', 'VUT', 'MKD', 'SVK', 'KIR', 'POL', 'CZE', 'BIH', 'TUN', 'GRL', 'KOR', 'ROU', 'ARG', 'SRB', 'NPL', 'PHL', 'NRU', 'COL', 'ARM', 'MEX', 'SMR', 'CHL', 'MAR', 'NAM', 'TON', 'RUS', 'YEM', 'HND', 'ZMB', 'KGZ', 'LKA', 'BWA', 'KEN', 'GMB', 'DOM', 'LSO', 'CIV', 'TUR', 'ECU', 'TGO', 'KAZ', 'LIE', 'XKX', 'SEN', 'SWZ', 'BOL', 'THA', 'BHR', 'IRL', 'PAK', 'CMR', 'PER', 'ALB', 'BLR', 'EGY', 'GNB', 'IND', 'KWT', 'BGD', 'IRN', 'PRI', 'MWI', 'LBY', 'COM', 'BTN', 'CHN', 'PRY', 'NIC', 'MDG', 'BEN', 'ERI', 'MYS', 'RWA', 'OMN', 'ZPM', 'HTI', 'TTO', 'SLB', 'DZA', 'ARE', 'AFG', 'COG', 'ZWE', 'LAO', 'MRT', 'MNG', 'SUR', 'NOR', 'TJK', 'VNM', 'MOZ', 'GHA', 'VEN', 'CAF', 'UGA', 'NGA', 'BDI', 'GNQ', 'TKM', 'IDN', 'BFA', 'SYR', 'UZB', 'SAU', 'AGO', 'PNG', 'GIN', 'MLI', 'MMR', 'NER', 'GAB', 'TZA', 'ETH', 'QAT', 'TLS', 'KHM', 'SSD', 'IRQ', 'AZE', 'PRK', 'SLE', 'COD', 'BRN', 'TCD', 'GUY', 'LBR', 'SOM', 'TUV', 'CPV', 'CSK', 'ETF', 'COK', 'MHL', 'SUV', 'SDN', 'YM1', 'YMD', 'ANT', 'SER'], col2='indicador', order2=['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios'])
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
#  latest_year(by='anio')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='KOS', new_value='XKX')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SD1', new_value='SDN')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='SV1', new_value='SUV')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ1', new_value='TZA')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='TZ2', new_value='ZPM')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='YUG', new_value='SER')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   gran_sector       1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   particip_va_pib   1439 non-null   float64
#  
#  |       | iso3   | iso3_desc_fundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:-------|:-------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG    | Afganistán         |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'particip_va_pib': 'valor', 'gran_sector': 'indicador'})
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         1537 non-null   object 
#   1   iso3_desc_fundar  1537 non-null   object 
#   2   es_agregacion     1537 non-null   int64  
#   3   letra             1537 non-null   object 
#   4   indicador         1537 non-null   object 
#   5   id_tipo_sector    1537 non-null   int64  
#   6   tipo_sector       1537 non-null   object 
#   7   valor             1439 non-null   float64
#  
#  |       | geocodigo   | iso3_desc_fundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |    valor |
#  |------:|:------------|:-------------------|----------------:|:--------|:-------------|-----------------:|:--------------|---------:|
#  | 79924 | AFG         | Afganistán         |               0 | AB      | Agro y pesca |                1 | Bienes        | 0.355494 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3_desc_fundar', 'es_agregacion', 'letra', 'id_tipo_sector', 'tipo_sector'], axis=1)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   geocodigo  1537 non-null   category
#   1   indicador  1537 non-null   category
#   2   valor      1439 non-null   float64 
#  
#  |       | geocodigo   | indicador    |   valor |
#  |------:|:------------|:-------------|--------:|
#  | 79924 | AFG         | Agro y pesca | 35.5494 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   geocodigo  1537 non-null   category
#   1   indicador  1537 non-null   category
#   2   valor      1439 non-null   float64 
#  
#  |       | geocodigo   | indicador    |   valor |
#  |------:|:------------|:-------------|--------:|
#  | 79924 | AFG         | Agro y pesca | 35.5494 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geocodigo', order1=['BMU', 'HKG', 'VGB', 'SXM', 'CYM', 'BHS', 'MAC', 'LUX', 'MCO', 'ABW', 'AND', 'AIA', 'DJI', 'LCA', 'CUW', 'MLT', 'PYF', 'CYP', 'PLW', 'TCA', 'MSR', 'BRB', 'SYC', 'MDV', 'LBN', 'GBR', 'USA', 'CUB', 'FRA', 'ISR', 'BEL', 'VCT', 'GRC', 'NLD', 'PRT', 'MNE', 'DNK', 'GRD', 'WSM', 'KNA', 'MUS', 'NCL', 'ATG', 'ESP', 'SGP', 'CHE', 'STP', 'CRI', 'CAN', 'HRV', 'BLZ', 'NZL', 'ITA', 'ISL', 'URY', 'SWE', 'PSE', 'FSM', 'PAN', 'EST', 'JPN', 'LVA', 'ZAF', 'AUT', 'DEU', 'UKR', 'GEO', 'AUS', 'BRA', 'FIN', 'JOR', 'JAM', 'MDA', 'LTU', 'HUN', 'BGR', 'SLV', 'FJI', 'DMA', 'GTM', 'SVN', 'VUT', 'MKD', 'SVK', 'KIR', 'POL', 'CZE', 'BIH', 'TUN', 'GRL', 'KOR', 'ROU', 'ARG', 'SRB', 'NPL', 'PHL', 'NRU', 'COL', 'ARM', 'MEX', 'SMR', 'CHL', 'MAR', 'NAM', 'TON', 'RUS', 'YEM', 'HND', 'ZMB', 'KGZ', 'LKA', 'BWA', 'KEN', 'GMB', 'DOM', 'LSO', 'CIV', 'TUR', 'ECU', 'TGO', 'KAZ', 'LIE', 'XKX', 'SEN', 'SWZ', 'BOL', 'THA', 'BHR', 'IRL', 'PAK', 'CMR', 'PER', 'ALB', 'BLR', 'EGY', 'GNB', 'IND', 'KWT', 'BGD', 'IRN', 'PRI', 'MWI', 'LBY', 'COM', 'BTN', 'CHN', 'PRY', 'NIC', 'MDG', 'BEN', 'ERI', 'MYS', 'RWA', 'OMN', 'ZPM', 'HTI', 'TTO', 'SLB', 'DZA', 'ARE', 'AFG', 'COG', 'ZWE', 'LAO', 'MRT', 'MNG', 'SUR', 'NOR', 'TJK', 'VNM', 'MOZ', 'GHA', 'VEN', 'CAF', 'UGA', 'NGA', 'BDI', 'GNQ', 'TKM', 'IDN', 'BFA', 'SYR', 'UZB', 'SAU', 'AGO', 'PNG', 'GIN', 'MLI', 'MMR', 'NER', 'GAB', 'TZA', 'ETH', 'QAT', 'TLS', 'KHM', 'SSD', 'IRQ', 'AZE', 'PRK', 'SLE', 'COD', 'BRN', 'TCD', 'GUY', 'LBR', 'SOM', 'TUV', 'CPV', 'CSK', 'ETF', 'COK', 'MHL', 'SUV', 'SDN', 'YM1', 'YMD', 'ANT', 'SER'], col2='indicador', order2=['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios'])
#  Index: 1537 entries, 80036 to 81451
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   geocodigo  1537 non-null   category
#   1   indicador  1537 non-null   category
#   2   valor      1439 non-null   float64 
#  
#  |       | geocodigo   | indicador    |    valor |
#  |------:|:------------|:-------------|---------:|
#  | 80036 | BMU         | Agro y pesca | 0.266278 |
#  
#  ------------------------------
#  