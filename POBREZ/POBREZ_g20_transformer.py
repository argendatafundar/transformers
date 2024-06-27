from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def filtrar_filas_ultimo_semestre(df:DataFrame, col_anio:str, col_semestre:str):
    df = df[df[col_anio] ==  df[col_anio].max()]
    df = df[df[col_semestre] == df[col_semestre].max()]
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='semester', curr_value='I', new_value=1),
	replace_value(col='date', curr_value='II', new_value=2),
	filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester'),
	query(condition="poverty_line == 'Pobreza'"),
	rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	drop_col(col='poverty_line', axis=1),
	replace_value(col='categoria', curr_value='old_and_child', new_value='Adultos mayores que viven con niños'),
	replace_value(col='categoria', curr_value='old_and_old', new_value='Adultos mayores que viven con otros adultos mayores'),
	replace_value(col='categoria', curr_value='child_no_siblings', new_value='Niños sin hermanos'),
	replace_value(col='categoria', curr_value='child_1_sibling', new_value='Niños con 1 hermano'),
	replace_value(col='categoria', curr_value='child_2more_siblings', new_value='Niños con 2 o más hermanos')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          400 non-null    int64  
#   1   semester      400 non-null    object 
#   2   age_group     400 non-null    object 
#   3   poverty_line  400 non-null    object 
#   4   poverty_rate  380 non-null    float64
#  
#  |    |   year | semester   | age_group     | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:--------------|:---------------|---------------:|
#  |  0 |   2003 | II         | old_and_child | Indigencia     |        21.0149 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value=1)
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          400 non-null    int64  
#   1   semester      400 non-null    object 
#   2   age_group     400 non-null    object 
#   3   poverty_line  400 non-null    object 
#   4   poverty_rate  380 non-null    float64
#  
#  |    |   year | semester   | age_group     | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:--------------|:---------------|---------------:|
#  |  0 |   2003 | II         | old_and_child | Indigencia     |        21.0149 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='II', new_value=2)
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          400 non-null    int64  
#   1   semester      400 non-null    object 
#   2   age_group     400 non-null    object 
#   3   poverty_line  400 non-null    object 
#   4   poverty_rate  380 non-null    float64
#  
#  |    |   year | semester   | age_group     | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:--------------|:---------------|---------------:|
#  |  0 |   2003 | II         | old_and_child | Indigencia     |        21.0149 |
#  
#  ------------------------------
#  
#  filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester')
#  Index: 10 entries, 195 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          10 non-null     int64  
#   1   semester      10 non-null     object 
#   2   age_group     10 non-null     object 
#   3   poverty_line  10 non-null     object 
#   4   poverty_rate  10 non-null     float64
#  
#  |     |   year |   semester | age_group     | poverty_line   |   poverty_rate |
#  |----:|-------:|-----------:|:--------------|:---------------|---------------:|
#  | 195 |   2023 |          1 | old_and_child | Indigencia     |        4.32701 |
#  
#  ------------------------------
#  
#  query(condition="poverty_line == 'Pobreza'")
#  Index: 5 entries, 395 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          5 non-null      int64  
#   1   semester      5 non-null      object 
#   2   age_group     5 non-null      object 
#   3   poverty_line  5 non-null      object 
#   4   poverty_rate  5 non-null      float64
#  
#  |     |   year |   semester | age_group     | poverty_line   |   poverty_rate |
#  |----:|-------:|-----------:|:--------------|:---------------|---------------:|
#  | 395 |   2023 |          1 | old_and_child | Pobreza        |        41.4502 |
#  
#  ------------------------------
#  
#  rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'})
#  Index: 5 entries, 395 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          5 non-null      int64  
#   1   semester      5 non-null      object 
#   2   categoria     5 non-null      object 
#   3   poverty_line  5 non-null      object 
#   4   valor         5 non-null      float64
#  
#  |     |   year |   semester | categoria     | poverty_line   |   valor |
#  |----:|-------:|-----------:|:--------------|:---------------|--------:|
#  | 395 |   2023 |          1 | old_and_child | Pobreza        | 41.4502 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 5 entries, 395 to 399
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   semester      5 non-null      object 
#   1   categoria     5 non-null      object 
#   2   poverty_line  5 non-null      object 
#   3   valor         5 non-null      float64
#  
#  |     |   semester | categoria     | poverty_line   |   valor |
#  |----:|-----------:|:--------------|:---------------|--------:|
#  | 395 |          1 | old_and_child | Pobreza        | 41.4502 |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 5 entries, 395 to 399
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     5 non-null      object 
#   1   poverty_line  5 non-null      object 
#   2   valor         5 non-null      float64
#  
#  |     | categoria     | poverty_line   |   valor |
#  |----:|:--------------|:---------------|--------:|
#  | 395 | old_and_child | Pobreza        | 41.4502 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria     |   valor |
#  |----:|:--------------|--------:|
#  | 395 | old_and_child | 41.4502 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='old_and_child', new_value='Adultos mayores que viven con niños')
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria                           |   valor |
#  |----:|:------------------------------------|--------:|
#  | 395 | Adultos mayores que viven con niños | 41.4502 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='old_and_old', new_value='Adultos mayores que viven con otros adultos mayores')
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria                           |   valor |
#  |----:|:------------------------------------|--------:|
#  | 395 | Adultos mayores que viven con niños | 41.4502 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='child_no_siblings', new_value='Niños sin hermanos')
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria                           |   valor |
#  |----:|:------------------------------------|--------:|
#  | 395 | Adultos mayores que viven con niños | 41.4502 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='child_1_sibling', new_value='Niños con 1 hermano')
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria                           |   valor |
#  |----:|:------------------------------------|--------:|
#  | 395 | Adultos mayores que viven con niños | 41.4502 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='child_2more_siblings', new_value='Niños con 2 o más hermanos')
#  Index: 5 entries, 395 to 399
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |     | categoria                           |   valor |
#  |----:|:------------------------------------|--------:|
#  | 395 | Adultos mayores que viven con niños | 41.4502 |
#  
#  ------------------------------
#  