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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='semester', curr_value='I', new_value=1),
	replace_value(col='date', curr_value='II', new_value=2),
	filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester'),
	rename_cols(map={'hh_gender': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	query(condition="indicador != 'Total'"),
	replace_value(col='indicador', curr_value='Jefe_Varon', new_value='Jefe Varón'),
	replace_value(col='indicador', curr_value='Jefa_Mujer', new_value='Jefa Mujer')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   hh_gender     240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | hh_gender   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total       | Indigencia     |         22.121 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value=1)
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   hh_gender     240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | hh_gender   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total       | Indigencia     |         22.121 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='II', new_value=2)
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   hh_gender     240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | hh_gender   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total       | Indigencia     |         22.121 |
#  
#  ------------------------------
#  
#  filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester')
#  Index: 6 entries, 117 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          6 non-null      int64  
#   1   semester      6 non-null      object 
#   2   hh_gender     6 non-null      object 
#   3   poverty_line  6 non-null      object 
#   4   poverty_rate  6 non-null      float64
#  
#  |     |   year |   semester | hh_gender   | poverty_line   |   poverty_rate |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|
#  | 117 |   2023 |          1 | Total       | Indigencia     |        9.37642 |
#  
#  ------------------------------
#  
#  rename_cols(map={'hh_gender': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'})
#  Index: 6 entries, 117 to 239
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       6 non-null      int64  
#   1   semester   6 non-null      object 
#   2   indicador  6 non-null      object 
#   3   categoria  6 non-null      object 
#   4   valor      6 non-null      float64
#  
#  |     |   year |   semester | indicador   | categoria   |   valor |
#  |----:|-------:|-----------:|:------------|:------------|--------:|
#  | 117 |   2023 |          1 | Total       | Indigencia  | 9.37642 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 6 entries, 117 to 239
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   semester   6 non-null      object 
#   1   indicador  6 non-null      object 
#   2   categoria  6 non-null      object 
#   3   valor      6 non-null      float64
#  
#  |     |   semester | indicador   | categoria   |   valor |
#  |----:|-----------:|:------------|:------------|--------:|
#  | 117 |          1 | Total       | Indigencia  | 9.37642 |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 6 entries, 117 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  6 non-null      object 
#   1   categoria  6 non-null      object 
#   2   valor      6 non-null      float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 117 | Total       | Indigencia  | 9.37642 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 4 entries, 118 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  4 non-null      object 
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 118 | Jefe_Varon  | Indigencia  | 7.79352 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Jefe_Varon', new_value='Jefe Varón')
#  Index: 4 entries, 118 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  4 non-null      object 
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 118 | Jefe Varón  | Indigencia  | 7.79352 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Jefa_Mujer', new_value='Jefa Mujer')
#  Index: 4 entries, 118 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  4 non-null      object 
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 118 | Jefe Varón  | Indigencia  | 7.79352 |
#  
#  ------------------------------
#  