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
	rename_cols(map={'educ_level': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	replace_value(col='indicador', curr_value='Primaria_o_menos', new_value='Primaria completa o menos'),
	replace_value(col='indicador', curr_value='Secu_incompleta', new_value='Secundaria incompleta'),
	replace_value(col='indicador', curr_value='Secu_completa', new_value='Secundaria completa'),
	replace_value(col='indicador', curr_value='Supe_incompleta', new_value='Superior incompleta'),
	replace_value(col='indicador', curr_value='Supe_completa', new_value='Superior completa')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          480 non-null    int64  
#   1   semester      480 non-null    object 
#   2   educ_level    480 non-null    object 
#   3   poverty_line  480 non-null    object 
#   4   poverty_rate  456 non-null    float64
#  
#  |    |   year | semester   | educ_level   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:-------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Todos        | Indigencia     |        18.3672 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value=1)
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          480 non-null    int64  
#   1   semester      480 non-null    object 
#   2   educ_level    480 non-null    object 
#   3   poverty_line  480 non-null    object 
#   4   poverty_rate  456 non-null    float64
#  
#  |    |   year | semester   | educ_level   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:-------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Todos        | Indigencia     |        18.3672 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='II', new_value=2)
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          480 non-null    int64  
#   1   semester      480 non-null    object 
#   2   educ_level    480 non-null    object 
#   3   poverty_line  480 non-null    object 
#   4   poverty_rate  456 non-null    float64
#  
#  |    |   year | semester   | educ_level   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:-------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Todos        | Indigencia     |        18.3672 |
#  
#  ------------------------------
#  
#  filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester')
#  Index: 12 entries, 234 to 479
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          12 non-null     int64  
#   1   semester      12 non-null     object 
#   2   educ_level    12 non-null     object 
#   3   poverty_line  12 non-null     object 
#   4   poverty_rate  12 non-null     float64
#  
#  |     |   year |   semester | educ_level   | poverty_line   |   poverty_rate |
#  |----:|-------:|-----------:|:-------------|:---------------|---------------:|
#  | 234 |   2023 |          1 | Todos        | Indigencia     |        8.01477 |
#  
#  ------------------------------
#  
#  rename_cols(map={'educ_level': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'})
#  Index: 12 entries, 234 to 479
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       12 non-null     int64  
#   1   semester   12 non-null     object 
#   2   indicador  12 non-null     object 
#   3   categoria  12 non-null     object 
#   4   valor      12 non-null     float64
#  
#  |     |   year |   semester | indicador   | categoria   |   valor |
#  |----:|-------:|-----------:|:------------|:------------|--------:|
#  | 234 |   2023 |          1 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 12 entries, 234 to 479
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   semester   12 non-null     object 
#   1   indicador  12 non-null     object 
#   2   categoria  12 non-null     object 
#   3   valor      12 non-null     float64
#  
#  |     |   semester | indicador   | categoria   |   valor |
#  |----:|-----------:|:------------|:------------|--------:|
#  | 234 |          1 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Primaria_o_menos', new_value='Primaria completa o menos')
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Secu_incompleta', new_value='Secundaria incompleta')
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Secu_completa', new_value='Secundaria completa')
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Supe_incompleta', new_value='Superior incompleta')
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Supe_completa', new_value='Superior completa')
#  Index: 12 entries, 234 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  12 non-null     object 
#   1   categoria  12 non-null     object 
#   2   valor      12 non-null     float64
#  
#  |     | indicador   | categoria   |   valor |
#  |----:|:------------|:------------|--------:|
#  | 234 | Todos       | Indigencia  | 8.01477 |
#  
#  ------------------------------
#  