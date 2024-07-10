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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='sitc_2_1_cod', axis=1),
	rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'}),
	replace_value(col='indicador', curr_value='Materiales crudos, no comestibles, excepto combustibles', new_value='Materiales crudos no comestibles y combustibles'),
	replace_value(col='indicador', curr_value='Combustibles minerales, lubricantes y productos similares', new_value='Combustibles minerales, lubricantes y similares'),
	replace_value(col='indicador', curr_value='Artículos manufacturados, clasificados principalmente según el material', new_value='Artículos manufacturados según material'),
	replace_value(col='indicador', curr_value='Transacciones y mercaderías diversas, N. E. P.', new_value='Transacciones y mercaderías diversas')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 122560 entries, 0 to 122559
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count   Dtype  
#  ---  ------                  --------------   -----  
#   0   year                    122560 non-null  int64  
#   1   iso3                    122560 non-null  object 
#   2   location_name_short_en  122560 non-null  object 
#   3   sitc_2_1_cod            122560 non-null  int64  
#   4   sitc_product_name_es    122560 non-null  object 
#   5   export_value_pc         122560 non-null  float64
#  
#  |    |   year | iso3   | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|-------:|:-------|:-------------------------|---------------:|:-----------------------|------------------:|
#  |  0 |   1962 | AFG    | Afghanistan              |              0 | Productos alimenticios |           22.4199 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    600 non-null    int64  
#   1   iso3                    600 non-null    object 
#   2   location_name_short_en  600 non-null    object 
#   3   sitc_2_1_cod            600 non-null    int64  
#   4   sitc_product_name_es    600 non-null    object 
#   5   export_value_pc         600 non-null    float64
#  
#  |    |   year | iso3   | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|-------:|:-------|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 |   1962 | ARG    | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    600 non-null    int64  
#   1   location_name_short_en  600 non-null    object 
#   2   sitc_2_1_cod            600 non-null    int64  
#   3   sitc_product_name_es    600 non-null    object 
#   4   export_value_pc         600 non-null    float64
#  
#  |    |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 |   1962 | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   year                  600 non-null    int64  
#   1   sitc_2_1_cod          600 non-null    int64  
#   2   sitc_product_name_es  600 non-null    object 
#   3   export_value_pc       600 non-null    float64
#  
#  |    |   year |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|-------:|---------------:|:-----------------------|------------------:|
#  | 50 |   1962 |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='sitc_2_1_cod', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   year                  600 non-null    int64  
#   1   sitc_product_name_es  600 non-null    object 
#   2   export_value_pc       600 non-null    float64
#  
#  |    |   year | sitc_product_name_es   |   export_value_pc |
#  |---:|-------:|:-----------------------|------------------:|
#  | 50 |   1962 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'})
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       600 non-null    int64  
#   1   indicador  600 non-null    object 
#   2   valor      600 non-null    float64
#  
#  |    |   anio | indicador              |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  | 50 |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Materiales crudos, no comestibles, excepto combustibles', new_value='Materiales crudos no comestibles y combustibles')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       600 non-null    int64  
#   1   indicador  600 non-null    object 
#   2   valor      600 non-null    float64
#  
#  |    |   anio | indicador              |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  | 50 |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Combustibles minerales, lubricantes y productos similares', new_value='Combustibles minerales, lubricantes y similares')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       600 non-null    int64  
#   1   indicador  600 non-null    object 
#   2   valor      600 non-null    float64
#  
#  |    |   anio | indicador              |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  | 50 |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Artículos manufacturados, clasificados principalmente según el material', new_value='Artículos manufacturados según material')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       600 non-null    int64  
#   1   indicador  600 non-null    object 
#   2   valor      600 non-null    float64
#  
#  |    |   anio | indicador              |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  | 50 |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Transacciones y mercaderías diversas, N. E. P.', new_value='Transacciones y mercaderías diversas')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       600 non-null    int64  
#   1   indicador  600 non-null    object 
#   2   valor      600 non-null    float64
#  
#  |    |   anio | indicador              |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  | 50 |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  