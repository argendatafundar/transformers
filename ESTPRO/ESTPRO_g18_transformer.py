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
query(condition='anio == 2020'),
	query(condition='iso3 == "AUS" or iso3 == "AUT" or iso3 == "BEL" or iso3 == "CAN" or iso3 == "CHE" or iso3 == "CHL" or iso3 == " CZE" or iso3 == " DEU" or iso3 =="DNK" or iso3 =="ESP" or iso3 =="EST" or iso3 =="FIN" or iso3 =="FRA" or iso3 =="GBR" or iso3 =="GRC" or iso3 =="HUN" or iso3 =="IRL" or iso3 =="ISL" or iso3 =="ISR" or iso3 =="ITA" or iso3 =="JPN" or iso3 =="KOR" or iso3 =="LUX" or iso3 =="MEX" or iso3 =="NLD" or iso3 =="NOR" or iso3 =="NZL" or iso3 =="POL" or iso3 =="PRT" or iso3 =="SVK" or iso3 =="SVN" or iso3 =="SWE" or iso3 =="TUR" or iso3 =="USA"'),
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
#  query(condition='anio == 2020')
#  Index: 1400 entries, 34999 to 36398
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
#  | 34999 |   2020 | A       | ARG    | Argentina          |               0 | Agro y pesca       |                  100 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "AUS" or iso3 == "AUT" or iso3 == "BEL" or iso3 == "CAN" or iso3 == "CHE" or iso3 == "CHL" or iso3 == " CZE" or iso3 == " DEU" or iso3 =="DNK" or iso3 =="ESP" or iso3 =="EST" or iso3 =="FIN" or iso3 =="FRA" or iso3 =="GBR" or iso3 =="GRC" or iso3 =="HUN" or iso3 =="IRL" or iso3 =="ISL" or iso3 =="ISR" or iso3 =="ITA" or iso3 =="JPN" or iso3 =="KOR" or iso3 =="LUX" or iso3 =="MEX" or iso3 =="NLD" or iso3 =="NOR" or iso3 =="NZL" or iso3 =="POL" or iso3 =="PRT" or iso3 =="SVK" or iso3 =="SVN" or iso3 =="SWE" or iso3 =="TUR" or iso3 =="USA"')
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                640 non-null    int64  
#   1   letra               640 non-null    object 
#   2   iso3                640 non-null    object 
#   3   iso3_desc_fundar    640 non-null    object 
#   4   es_agregacion       640 non-null    int64  
#   5   letra_desc_abrev    640 non-null    object 
#   6   valor_relativo_arg  640 non-null    float64
#  
#  |       |   anio | letra   | iso3   | iso3_desc_fundar   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |------:|-------:|:--------|:-------|:-------------------|----------------:|:-------------------|---------------------:|
#  | 35000 |   2020 | A       | AUS    | Australia          |               0 | Agro y pesca       |              591.287 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria', 'valor_relativo_arg': 'valor', 'iso3': 'geocodigo'})
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 7 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              640 non-null    int64  
#   1   letra             640 non-null    object 
#   2   geocodigo         640 non-null    object 
#   3   iso3_desc_fundar  640 non-null    object 
#   4   es_agregacion     640 non-null    int64  
#   5   categoria         640 non-null    object 
#   6   valor             640 non-null    float64
#  
#  |       |   anio | letra   | geocodigo   | iso3_desc_fundar   |   es_agregacion | categoria    |   valor |
#  |------:|-------:|:--------|:------------|:-------------------|----------------:|:-------------|--------:|
#  | 35000 |   2020 | A       | AUS         | Australia          |               0 | Agro y pesca | 591.287 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              640 non-null    int64  
#   1   letra             640 non-null    object 
#   2   geocodigo         640 non-null    object 
#   3   iso3_desc_fundar  640 non-null    object 
#   4   categoria         640 non-null    object 
#   5   valor             640 non-null    float64
#  
#  |       |   anio | letra   | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|-------:|:--------|:------------|:-------------------|:-------------|--------:|
#  | 35000 |   2020 | A       | AUS         | Australia          | Agro y pesca | 591.287 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             640 non-null    object 
#   1   geocodigo         640 non-null    object 
#   2   iso3_desc_fundar  640 non-null    object 
#   3   categoria         640 non-null    object 
#   4   valor             640 non-null    float64
#  
#  |       | letra   | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|:--------|:------------|:-------------------|:-------------|--------:|
#  | 35000 | A       | AUS         | Australia          | Agro y pesca | 591.287 |
#  
#  ------------------------------
#  
#  drop_col(col='letra', axis=1)
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         640 non-null    object 
#   1   iso3_desc_fundar  640 non-null    object 
#   2   categoria         640 non-null    object 
#   3   valor             640 non-null    float64
#  
#  |       | geocodigo   | iso3_desc_fundar   | categoria    |   valor |
#  |------:|:------------|:-------------------|:-------------|--------:|
#  | 35000 | AUS         | Australia          | Agro y pesca | 591.287 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  Index: 640 entries, 35000 to 36394
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  640 non-null    object 
#   1   categoria  640 non-null    object 
#   2   valor      640 non-null    float64
#  
#  |       | geocodigo   | categoria    |   valor |
#  |------:|:------------|:-------------|--------:|
#  | 35000 | AUS         | Agro y pesca | 591.287 |
#  
#  ------------------------------
#  