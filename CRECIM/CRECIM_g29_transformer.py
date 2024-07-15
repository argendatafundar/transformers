from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
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
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3.isin( ['USA','GBR','ARG','AUS','WRL_MPD','WEU_MPD'])"),
	drop_col(col='continente_fundar', axis=1),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'categoria', 'pib_per_capita': 'valor'}),
	drop_na(col=['valor']),
	replace_value(col='categoria', curr_value='WRL_MPD', new_value='Mundo'),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='AUS', new_value='Australia'),
	replace_value(col='categoria', curr_value='GBR', new_value='Reino Unido'),
	replace_value(col='categoria', curr_value='USA', new_value='Estados Unidos'),
	replace_value(col='categoria', curr_value='WEU_MPD', new_value='Europa Occidental'),
	sort_values(how='ascending', by=['anio', 'categoria']),
	query(condition='anio >= 1900')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               21618 non-null  object 
#   1   pais_nombre        21618 non-null  object 
#   2   continente_fundar  21366 non-null  object 
#   3   es_agregacion      21618 non-null  int64  
#   4   anio               21618 non-null  int64  
#   5   pib_per_capita     21586 non-null  float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|-----------------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1950 |             1156 |
#  
#  ------------------------------
#  
#  query(condition="iso3.isin( ['USA','GBR','ARG','AUS','WRL_MPD','WEU_MPD'])")
#  Index: 1410 entries, 296 to 20949
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               1410 non-null   object 
#   1   pais_nombre        1410 non-null   object 
#   2   continente_fundar  1354 non-null   object 
#   3   es_agregacion      1410 non-null   int64  
#   4   anio               1410 non-null   int64  
#   5   pib_per_capita     1403 non-null   float64
#  
#  |     | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |
#  |----:|:-------|:--------------|:--------------------|----------------:|-------:|-----------------:|
#  | 296 | ARG    | Argentina     | América del Sur     |               0 |   1800 |             1484 |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  Index: 1410 entries, 296 to 20949
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            1410 non-null   object 
#   1   pais_nombre     1410 non-null   object 
#   2   es_agregacion   1410 non-null   int64  
#   3   anio            1410 non-null   int64  
#   4   pib_per_capita  1403 non-null   float64
#  
#  |     | iso3   | pais_nombre   |   es_agregacion |   anio |   pib_per_capita |
#  |----:|:-------|:--------------|----------------:|-------:|-----------------:|
#  | 296 | ARG    | Argentina     |               0 |   1800 |             1484 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  Index: 1410 entries, 296 to 20949
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            1410 non-null   object 
#   1   pais_nombre     1410 non-null   object 
#   2   anio            1410 non-null   int64  
#   3   pib_per_capita  1403 non-null   float64
#  
#  |     | iso3   | pais_nombre   |   anio |   pib_per_capita |
#  |----:|:-------|:--------------|-------:|-----------------:|
#  | 296 | ARG    | Argentina     |   1800 |             1484 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  Index: 1410 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            1410 non-null   object 
#   1   anio            1410 non-null   int64  
#   2   pib_per_capita  1403 non-null   float64
#  
#  |     | iso3   |   anio |   pib_per_capita |
#  |----:|:-------|-------:|-----------------:|
#  | 296 | ARG    |   1800 |             1484 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'categoria', 'pib_per_capita': 'valor'})
#  Index: 1410 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1410 non-null   object 
#   1   anio       1410 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | ARG         |   1800 |    1484 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | ARG         |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='WRL_MPD', new_value='Mundo')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | ARG         |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='AUS', new_value='Australia')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GBR', new_value='Reino Unido')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='USA', new_value='Estados Unidos')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='WEU_MPD', new_value='Europa Occidental')
#  Index: 1403 entries, 296 to 20949
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 1403 entries, 0 to 1402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Reino Unido |   1000 |    1151 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1900')
#  Index: 530 entries, 873 to 1402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  530 non-null    object 
#   1   anio       530 non-null    int64  
#   2   valor      530 non-null    float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 873 | Argentina   |   1900 |    4583 |
#  
#  ------------------------------
#  