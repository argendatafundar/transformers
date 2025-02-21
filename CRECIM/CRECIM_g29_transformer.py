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
query(condition="iso3.isin( ['USA','GBR','ARG','AUS','WLD','WEU_MPD'])"),
	rename_cols(map={'iso3': 'categoria', 'pib_per_capita': 'valor'}),
	drop_na(col=['valor']),
	replace_value(col='categoria', curr_value='WLD', new_value='Mundo'),
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
#  RangeIndex: 21586 entries, 0 to 21585
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            21586 non-null  int64  
#   1   iso3            21586 non-null  object 
#   2   pib_per_capita  21586 non-null  float64
#  
#  |    |   anio | iso3   |   pib_per_capita |
#  |---:|-------:|:-------|-----------------:|
#  |  0 |   1950 | AFG    |             1156 |
#  
#  ------------------------------
#  
#  query(condition="iso3.isin( ['USA','GBR','ARG','AUS','WLD','WEU_MPD'])")
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            1403 non-null   int64  
#   1   iso3            1403 non-null   object 
#   2   pib_per_capita  1403 non-null   float64
#  
#  |     |   anio | iso3   |   pib_per_capita |
#  |----:|-------:|:-------|-----------------:|
#  | 296 |   1800 | ARG    |             1484 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'categoria', 'pib_per_capita': 'valor'})
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | ARG         |    1484 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | ARG         |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='WLD', new_value='Mundo')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | ARG         |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | Argentina   |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='AUS', new_value='Australia')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | Argentina   |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GBR', new_value='Reino Unido')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | Argentina   |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='USA', new_value='Estados Unidos')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | Argentina   |    1484 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='WEU_MPD', new_value='Europa Occidental')
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 296 |   1800 | Argentina   |    1484 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 1403 entries, 0 to 1402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1403 non-null   int64  
#   1   categoria  1403 non-null   object 
#   2   valor      1403 non-null   float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1000 | Reino Unido |    1151 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1900')
#  Index: 530 entries, 873 to 1402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       530 non-null    int64  
#   1   categoria  530 non-null    object 
#   2   valor      530 non-null    float64
#  
#  |     |   anio | categoria   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 873 |   1900 | Argentina   |    4583 |
#  
#  ------------------------------
#  