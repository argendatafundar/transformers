from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

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

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	latest_year(by='anio'),
	rename_cols(map={'particip_va_pib': 'valor', 'gran_sector': 'indicador'}),
	multiplicar_por_escalar(col='valor', k=100),
	ordenar_dos_columnas(col1='indicador', order1=['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios'], col2='geocodigoFundar', order2=['SER', 'SOM', 'TUV', 'CPV', 'CSK', 'ETF', 'ANT', 'MHL', 'YMD', 'YM1', 'COK', 'SUV', 'SDN', 'LBR', 'GUY', 'TCD', 'BRN', 'COD', 'SLE', 'PRK', 'AZE', 'IRQ', 'SSD', 'KHM', 'TLS', 'QAT', 'ETH', 'TZA', 'GAB', 'NER', 'MMR', 'MLI', 'GIN', 'PNG', 'AGO', 'SAU', 'UZB', 'SYR', 'BFA', 'IDN', 'TKM', 'GNQ', 'BDI', 'NGA', 'UGA', 'CAF', 'VEN', 'GHA', 'MOZ', 'VNM', 'TJK', 'NOR', 'SUR', 'MNG', 'MRT', 'LAO', 'ZWE', 'COG', 'AFG', 'ARE', 'DZA', 'SLB', 'TTO', 'HTI', 'ZPM', 'OMN', 'RWA', 'MYS', 'ERI', 'BEN', 'MDG', 'NIC', 'PRY', 'CHN', 'BTN', 'COM', 'LBY', 'MWI', 'PRI', 'IRN', 'BGD', 'KWT', 'IND', 'GNB', 'EGY', 'BLR', 'ALB', 'PER', 'CMR', 'PAK', 'IRL', 'BHR', 'THA', 'BOL', 'SWZ', 'SEN', 'XKX', 'LIE', 'KAZ', 'TGO', 'ECU', 'TUR', 'CIV', 'LSO', 'DOM', 'GMB', 'KEN', 'BWA', 'LKA', 'KGZ', 'ZMB', 'HND', 'YEM', 'RUS', 'TON', 'NAM', 'MAR', 'CHL', 'SMR', 'MEX', 'ARM', 'COL', 'NRU', 'PHL', 'NPL', 'SRB', 'ARG', 'ROU', 'KOR', 'GRL', 'TUN', 'BIH', 'CZE', 'POL', 'KIR', 'SVK', 'MKD', 'VUT', 'SVN', 'GTM', 'DMA', 'FJI', 'SLV', 'BGR', 'HUN', 'LTU', 'MDA', 'JAM', 'JOR', 'FIN', 'BRA', 'AUS', 'GEO', 'UKR', 'DEU', 'AUT', 'ZAF', 'LVA', 'JPN', 'EST', 'PAN', 'FSM', 'PSE', 'SWE', 'URY', 'ISL', 'ITA', 'NZL', 'BLZ', 'HRV', 'CAN', 'CRI', 'STP', 'CHE', 'SGP', 'ESP', 'ATG', 'NCL', 'MUS', 'KNA', 'WSM', 'GRD', 'DNK', 'MNE', 'PRT', 'NLD', 'GRC', 'VCT', 'BEL', 'ISR', 'FRA', 'CUB', 'USA', 'GBR', 'LBN', 'MDV', 'SYC', 'BRB', 'MSR', 'TCA', 'PLW', 'CYP', 'PYF', 'MLT', 'CUW', 'LCA', 'DJI', 'AIA', 'AND', 'ABW', 'MCO', 'LUX', 'MAC', 'BHS', 'CYM', 'SXM', 'VGB', 'HKG', 'BMU']),
	drop_na(subset='valor')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 81461 entries, 0 to 81460
#  Data columns (total 9 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  81461 non-null  object 
#   1   geonombreFundar  81461 non-null  object 
#   2   anio             81461 non-null  int64  
#   3   es_agregacion    81461 non-null  int64  
#   4   letra            81461 non-null  object 
#   5   gran_sector      81461 non-null  object 
#   6   id_tipo_sector   81461 non-null  int64  
#   7   tipo_sector      81461 non-null  object 
#   8   particip_va_pib  73145 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |---:|:------------------|:------------------|-------:|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  |  0 | AFG               | Afganistán        |   1970 |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.502422 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1537 non-null   object 
#   1   geonombreFundar  1537 non-null   object 
#   2   es_agregacion    1537 non-null   int64  
#   3   letra            1537 non-null   object 
#   4   gran_sector      1537 non-null   object 
#   5   id_tipo_sector   1537 non-null   int64  
#   6   tipo_sector      1537 non-null   object 
#   7   particip_va_pib  1439 non-null   float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:------------------|:------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  rename_cols(map={'particip_va_pib': 'valor', 'gran_sector': 'indicador'})
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1516 non-null   category
#   1   geonombreFundar  1537 non-null   object  
#   2   es_agregacion    1537 non-null   int64   
#   3   letra            1537 non-null   object  
#   4   indicador        1537 non-null   category
#   5   id_tipo_sector   1537 non-null   int64   
#   6   tipo_sector      1537 non-null   object  
#   7   valor            1439 non-null   float64 
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca |                1 | Bienes        | 35.5494 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1516 non-null   category
#   1   geonombreFundar  1537 non-null   object  
#   2   es_agregacion    1537 non-null   int64   
#   3   letra            1537 non-null   object  
#   4   indicador        1537 non-null   category
#   5   id_tipo_sector   1537 non-null   int64   
#   6   tipo_sector      1537 non-null   object  
#   7   valor            1439 non-null   float64 
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca |                1 | Bienes        | 35.5494 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='indicador', order1=['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios'], col2='geocodigoFundar', order2=['SER', 'SOM', 'TUV', 'CPV', 'CSK', 'ETF', 'ANT', 'MHL', 'YMD', 'YM1', 'COK', 'SUV', 'SDN', 'LBR', 'GUY', 'TCD', 'BRN', 'COD', 'SLE', 'PRK', 'AZE', 'IRQ', 'SSD', 'KHM', 'TLS', 'QAT', 'ETH', 'TZA', 'GAB', 'NER', 'MMR', 'MLI', 'GIN', 'PNG', 'AGO', 'SAU', 'UZB', 'SYR', 'BFA', 'IDN', 'TKM', 'GNQ', 'BDI', 'NGA', 'UGA', 'CAF', 'VEN', 'GHA', 'MOZ', 'VNM', 'TJK', 'NOR', 'SUR', 'MNG', 'MRT', 'LAO', 'ZWE', 'COG', 'AFG', 'ARE', 'DZA', 'SLB', 'TTO', 'HTI', 'ZPM', 'OMN', 'RWA', 'MYS', 'ERI', 'BEN', 'MDG', 'NIC', 'PRY', 'CHN', 'BTN', 'COM', 'LBY', 'MWI', 'PRI', 'IRN', 'BGD', 'KWT', 'IND', 'GNB', 'EGY', 'BLR', 'ALB', 'PER', 'CMR', 'PAK', 'IRL', 'BHR', 'THA', 'BOL', 'SWZ', 'SEN', 'XKX', 'LIE', 'KAZ', 'TGO', 'ECU', 'TUR', 'CIV', 'LSO', 'DOM', 'GMB', 'KEN', 'BWA', 'LKA', 'KGZ', 'ZMB', 'HND', 'YEM', 'RUS', 'TON', 'NAM', 'MAR', 'CHL', 'SMR', 'MEX', 'ARM', 'COL', 'NRU', 'PHL', 'NPL', 'SRB', 'ARG', 'ROU', 'KOR', 'GRL', 'TUN', 'BIH', 'CZE', 'POL', 'KIR', 'SVK', 'MKD', 'VUT', 'SVN', 'GTM', 'DMA', 'FJI', 'SLV', 'BGR', 'HUN', 'LTU', 'MDA', 'JAM', 'JOR', 'FIN', 'BRA', 'AUS', 'GEO', 'UKR', 'DEU', 'AUT', 'ZAF', 'LVA', 'JPN', 'EST', 'PAN', 'FSM', 'PSE', 'SWE', 'URY', 'ISL', 'ITA', 'NZL', 'BLZ', 'HRV', 'CAN', 'CRI', 'STP', 'CHE', 'SGP', 'ESP', 'ATG', 'NCL', 'MUS', 'KNA', 'WSM', 'GRD', 'DNK', 'MNE', 'PRT', 'NLD', 'GRC', 'VCT', 'BEL', 'ISR', 'FRA', 'CUB', 'USA', 'GBR', 'LBN', 'MDV', 'SYC', 'BRB', 'MSR', 'TCA', 'PLW', 'CYP', 'PYF', 'MLT', 'CUW', 'LCA', 'DJI', 'AIA', 'AND', 'ABW', 'MCO', 'LUX', 'MAC', 'BHS', 'CYM', 'SXM', 'VGB', 'HKG', 'BMU'])
#  Index: 1537 entries, 81167 to 81451
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1516 non-null   category
#   1   geonombreFundar  1537 non-null   object  
#   2   es_agregacion    1537 non-null   int64   
#   3   letra            1537 non-null   object  
#   4   indicador        1537 non-null   category
#   5   id_tipo_sector   1537 non-null   int64   
#   6   tipo_sector      1537 non-null   object  
#   7   valor            1439 non-null   float64 
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 81167 | SOM               | Somalia           |               0 | AB      | Agro y pesca |                1 | Bienes        |     nan |
#  
#  ------------------------------
#  
#  drop_na(subset='valor')
#  Index: 1439 entries, 80673 to 81381
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1432 non-null   category
#   1   geonombreFundar  1439 non-null   object  
#   2   es_agregacion    1439 non-null   int64   
#   3   letra            1439 non-null   object  
#   4   indicador        1439 non-null   category
#   5   id_tipo_sector   1439 non-null   int64   
#   6   tipo_sector      1439 non-null   object  
#   7   valor            1439 non-null   float64 
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 80673 | LBR               | Liberia           |               0 | AB      | Agro y pesca |                1 | Bienes        | 73.9551 |
#  
#  ------------------------------
#  