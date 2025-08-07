from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='sitc_2_1_cod', axis=1),
	rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'}),
	replace_value(col='indicador', curr_value='Materiales crudos, no comestibles, excepto combustibles', new_value='Materiales crudos no comestibles'),
	replace_value(col='indicador', curr_value='Combustibles minerales, lubricantes y productos similares', new_value='Combustibles minerales, lubricantes y similares'),
	replace_value(col='indicador', curr_value='Artículos manufacturados, clasificados principalmente según el material', new_value='Artículos manufacturados según material'),
	replace_value(col='indicador', curr_value='Transacciones y mercaderías diversas, N. E. P.', new_value='Transacciones y mercaderías diversas')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 122560 entries, 0 to 122559
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count   Dtype  
#  ---  ------                  --------------   -----  
#   0   geocodigoFundar         122560 non-null  object 
#   1   geonombreFundar         122560 non-null  object 
#   2   year                    122560 non-null  int64  
#   3   location_name_short_en  122560 non-null  object 
#   4   sitc_2_1_cod            122560 non-null  int64  
#   5   sitc_product_name_es    122560 non-null  object 
#   6   export_value_pc         122560 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  |  0 | AFG               | Afganistán        |   1962 | Afghanistan              |              0 | Productos alimenticios |           22.4199 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         600 non-null    object 
#   1   geonombreFundar         600 non-null    object 
#   2   year                    600 non-null    int64  
#   3   location_name_short_en  600 non-null    object 
#   4   sitc_2_1_cod            600 non-null    int64  
#   5   sitc_product_name_es    600 non-null    object 
#   6   export_value_pc         600 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 | ARG               | Argentina         |   1962 | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         600 non-null    object 
#   1   year                    600 non-null    int64  
#   2   location_name_short_en  600 non-null    object 
#   3   sitc_2_1_cod            600 non-null    int64  
#   4   sitc_product_name_es    600 non-null    object 
#   5   export_value_pc         600 non-null    float64
#  
#  |    | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geonombreFundar       600 non-null    object 
#   1   year                  600 non-null    int64  
#   2   sitc_2_1_cod          600 non-null    int64  
#   3   sitc_product_name_es  600 non-null    object 
#   4   export_value_pc       600 non-null    float64
#  
#  |    | geonombreFundar   |   year |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|---------------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='sitc_2_1_cod', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geonombreFundar       600 non-null    object 
#   1   year                  600 non-null    int64  
#   2   sitc_product_name_es  600 non-null    object 
#   3   export_value_pc       600 non-null    float64
#  
#  |    | geonombreFundar   |   year | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'})
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Materiales crudos, no comestibles, excepto combustibles', new_value='Materiales crudos no comestibles')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Combustibles minerales, lubricantes y productos similares', new_value='Combustibles minerales, lubricantes y similares')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Artículos manufacturados, clasificados principalmente según el material', new_value='Artículos manufacturados según material')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Transacciones y mercaderías diversas, N. E. P.', new_value='Transacciones y mercaderías diversas')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  