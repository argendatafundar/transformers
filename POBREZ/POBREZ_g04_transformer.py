from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def filtrar_filas_ultimo_semestre(df:DataFrame, col_anio:str, col_semestre:str):
    df = df[df[col_anio] ==  df[col_anio].max()]
    df = df[df[col_semestre] == df[col_semestre].max()]
    return df

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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	replace_value(col='semester', curr_value='I', new_value=1),
	replace_value(col='date', curr_value='II', new_value=2),
	filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester'),
	rename_cols(map={'region': 'indicador', 'k_value': 'categoria', 'pov_rate': 'valor'}),
	replace_value(col='categoria', curr_value=0.25, new_value='k = 0,25'),
	replace_value(col='categoria', curr_value=0.35, new_value='k = 0,35'),
	replace_value(col='indicador', curr_value='Partidos', new_value='Part. del GBA'),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	query(condition="indicador != 'Total'"),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      672 non-null    int64  
#   1   semester  672 non-null    object 
#   2   region    672 non-null    object 
#   3   k_value   672 non-null    float64
#   4   pov_rate  640 non-null    float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value=1)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      672 non-null    int64  
#   1   semester  672 non-null    object 
#   2   region    672 non-null    object 
#   3   k_value   672 non-null    float64
#   4   pov_rate  640 non-null    float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='II', new_value=2)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      672 non-null    int64  
#   1   semester  672 non-null    object 
#   2   region    672 non-null    object 
#   3   k_value   672 non-null    float64
#   4   pov_rate  640 non-null    float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  filtrar_filas_ultimo_semestre(col_anio='year', col_semestre='semester')
#  Index: 16 entries, 328 to 671
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      16 non-null     int64  
#   1   semester  16 non-null     object 
#   2   region    16 non-null     object 
#   3   k_value   16 non-null     float64
#   4   pov_rate  16 non-null     float64
#  
#  |     |   year |   semester | region   |   k_value |   pov_rate |
#  |----:|-------:|-----------:|:---------|----------:|-----------:|
#  | 328 |   2024 |          1 | Total    |      0.25 |  0.0953836 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'indicador', 'k_value': 'categoria', 'pov_rate': 'valor'})
#  Index: 16 entries, 328 to 671
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       16 non-null     int64  
#   1   semester   16 non-null     object 
#   2   indicador  16 non-null     object 
#   3   categoria  16 non-null     float64
#   4   valor      16 non-null     float64
#  
#  |     |   year |   semester | indicador   |   categoria |     valor |
#  |----:|-------:|-----------:|:------------|------------:|----------:|
#  | 328 |   2024 |          1 | Total       |        0.25 | 0.0953836 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value=0.25, new_value='k = 0,25')
#  Index: 16 entries, 328 to 671
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       16 non-null     int64  
#   1   semester   16 non-null     object 
#   2   indicador  16 non-null     object 
#   3   categoria  16 non-null     object 
#   4   valor      16 non-null     float64
#  
#  |     |   year |   semester | indicador   | categoria   |     valor |
#  |----:|-------:|-----------:|:------------|:------------|----------:|
#  | 328 |   2024 |          1 | Total       | k = 0,25    | 0.0953836 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value=0.35, new_value='k = 0,35')
#  Index: 16 entries, 328 to 671
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       16 non-null     int64  
#   1   semester   16 non-null     object 
#   2   indicador  16 non-null     object 
#   3   categoria  16 non-null     object 
#   4   valor      16 non-null     float64
#  
#  |     |   year |   semester | indicador   | categoria   |     valor |
#  |----:|-------:|-----------:|:------------|:------------|----------:|
#  | 328 |   2024 |          1 | Total       | k = 0,25    | 0.0953836 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Partidos', new_value='Part. del GBA')
#  Index: 16 entries, 328 to 671
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       16 non-null     int64  
#   1   semester   16 non-null     object 
#   2   indicador  16 non-null     object 
#   3   categoria  16 non-null     object 
#   4   valor      16 non-null     float64
#  
#  |     |   year |   semester | indicador   | categoria   |     valor |
#  |----:|-------:|-----------:|:------------|:------------|----------:|
#  | 328 |   2024 |          1 | Total       | k = 0,25    | 0.0953836 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 16 entries, 328 to 671
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   semester   16 non-null     object 
#   1   indicador  16 non-null     object 
#   2   categoria  16 non-null     object 
#   3   valor      16 non-null     float64
#  
#  |     |   semester | indicador   | categoria   |     valor |
#  |----:|-----------:|:------------|:------------|----------:|
#  | 328 |          1 | Total       | k = 0,25    | 0.0953836 |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 16 entries, 328 to 671
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  16 non-null     object 
#   1   categoria  16 non-null     object 
#   2   valor      16 non-null     float64
#  
#  |     | indicador   | categoria   |     valor |
#  |----:|:------------|:------------|----------:|
#  | 328 | Total       | k = 0,25    | 0.0953836 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 14 entries, 329 to 671
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  14 non-null     object 
#   1   categoria  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |     | indicador     | categoria   |   valor |
#  |----:|:--------------|:------------|--------:|
#  | 329 | Part. del GBA | k = 0,25    | 13.7982 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 14 entries, 329 to 671
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  14 non-null     object 
#   1   categoria  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |     | indicador     | categoria   |   valor |
#  |----:|:--------------|:------------|--------:|
#  | 329 | Part. del GBA | k = 0,25    | 13.7982 |
#  
#  ------------------------------
#  