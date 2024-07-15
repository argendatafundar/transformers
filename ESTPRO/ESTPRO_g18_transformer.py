from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2019'),
	query(condition='iso3.isin(["AUS", "NZL", "AUT", "NOR", "BEL", "POL", "CAN", "PRT", "CHL", "SVK", "CZE", "SVN", "DNK", "ESP", "EST", "SWE", "FIN", "CHE", "FRA", "TUR", "DEU", "GBR", "GRC", "USA", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN", "KOR", "LUX", "MEX",  "NLD"])'),
	rename_cols(map={'letra_desc_abrev': 'categoria', 'valor_relativo_arg': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='anio', axis=1),
	drop_col(col='letra', axis=1),
	drop_col(col='iso3_desc_fundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36399 entries, 0 to 36398
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                36399 non-null  int64  
#   1   letra               36399 non-null  object 
#   2   iso3                36399 non-null  object 
#   3   iso3_desc_fundar    36399 non-null  object 
#   4   es_agregacion       36399 non-null  int64  
#   5   letra_desc_abrev    36399 non-null  object 
#   6   valor_relativo_arg  36399 non-null  float64
#  
#  |    |   anio | letra   | iso3   | iso3_desc_fundar   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |---:|-------:|:--------|:-------|:-------------------|----------------:|:-------------------|---------------------:|
#  |  0 |   1995 | A       | ARG    | Argentina          |               0 | Agro y pesca       |                  100 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2019')
#  Index: 1400 entries, 33599 to 34998
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                1400 non-null   int64  
#   1   letra               1400 non-null   object 
#   2   iso3                1400 non-null   object 
#   3   iso3_desc_fundar    1400 non-null   object 
#   4   es_agregacion       1400 non-null   int64  
#   5   letra_desc_abrev    1400 non-null   object 
#   6   valor_relativo_arg  1400 non-null   float64
#  
#  |       |   anio | letra   | iso3   | iso3_desc_fundar   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |------:|-------:|:--------|:-------|:-------------------|----------------:|:-------------------|---------------------:|
#  | 33599 |   2019 | A       | ARG    | Argentina          |               0 | Agro y pesca       |                  100 |
#  
#  ------------------------------
#  
#  query(condition='iso3.isin(["AUS", "NZL", "AUT", "NOR", "BEL", "POL", "CAN", "PRT", "CHL", "SVK", "CZE", "SVN", "DNK", "ESP", "EST", "SWE", "FIN", "CHE", "FRA", "TUR", "DEU", "GBR", "GRC", "USA", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN", "KOR", "LUX", "MEX",  "NLD"])')
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                680 non-null    int64  
#   1   letra               680 non-null    object 
#   2   iso3                680 non-null    object 
#   3   iso3_desc_fundar    680 non-null    object 
#   4   es_agregacion       680 non-null    int64  
#   5   letra_desc_abrev    680 non-null    object 
#   6   valor_relativo_arg  680 non-null    float64
#  
#  |       |   anio | letra   | iso3   | iso3_desc_fundar   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |------:|-------:|:--------|:-------|:-------------------|----------------:|:-------------------|---------------------:|
#  | 33600 |   2019 | A       | AUS    | Australia          |               0 | Agro y pesca       |              555.496 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria', 'valor_relativo_arg': 'valor', 'iso3': 'geocodigo'})
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              680 non-null    int64  
#   1   letra             680 non-null    object 
#   2   geocodigo         680 non-null    object 
#   3   iso3_desc_fundar  680 non-null    object 
#   4   es_agregacion     680 non-null    int64  
#   5   categoria         680 non-null    object 
#   6   valor             680 non-null    float64
#  
#  |       |   anio | letra   | geocodigo   | iso3_desc_fundar   |   es_agregacion | categoria    |   valor |
#  |------:|-------:|:--------|:------------|:-------------------|----------------:|:-------------|--------:|
#  | 33600 |   2019 | A       | AUS         | Australia          |               0 | Agro y pesca | 555.496 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              680 non-null    int64  
#   1   letra             680 non-null    object 
#   2   geocodigo         680 non-null    object 
#   3   iso3_desc_fundar  680 non-null    object 
#   4   categoria         680 non-null    object 
#   5   valor             680 non-null    float64
#  
#  |       |   anio | letra   | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|-------:|:--------|:------------|:-------------------|:-------------|--------:|
#  | 33600 |   2019 | A       | AUS         | Australia          | Agro y pesca | 555.496 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             680 non-null    object 
#   1   geocodigo         680 non-null    object 
#   2   iso3_desc_fundar  680 non-null    object 
#   3   categoria         680 non-null    object 
#   4   valor             680 non-null    float64
#  
#  |       | letra   | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|:--------|:------------|:-------------------|:-------------|--------:|
#  | 33600 | A       | AUS         | Australia          | Agro y pesca | 555.496 |
#  
#  ------------------------------
#  
#  drop_col(col='letra', axis=1)
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         680 non-null    object 
#   1   iso3_desc_fundar  680 non-null    object 
#   2   categoria         680 non-null    object 
#   3   valor             680 non-null    float64
#  
#  |       | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|:------------|:-------------------|:-------------|--------:|
#  | 33600 | AUS         | Australia          | Agro y pesca | 555.496 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  Index: 680 entries, 33600 to 34994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  680 non-null    object 
#   1   categoria  680 non-null    object 
#   2   valor      680 non-null    float64
#  
#  |       | geocodigo   | categoria    |   valor |
#  |------:|:------------|:-------------|--------:|
#  | 33600 | AUS         | Agro y pesca | 555.496 |
#  
#  ------------------------------
#  