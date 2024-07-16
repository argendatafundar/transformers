from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
drop_col(col='year', axis=1),
	drop_col(col='semestre', axis=1),
	rename_cols(map={'deflactor': 'indicador', 'region': 'categoria', 'indice': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='ipc', new_value='Ingreso per cápita'),
	replace_value(col='indicador', curr_value='linea de pobreza', new_value='Ingreso per cápita deflactado por línea de pobreza')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       14 non-null     int64  
#   1   semestre   14 non-null     int64  
#   2   region     14 non-null     object 
#   3   deflactor  14 non-null     object 
#   4   indice     14 non-null     float64
#  
#  |    |   year |   semestre | region           | deflactor   |   indice |
#  |---:|-------:|-----------:|:-----------------|:------------|---------:|
#  |  0 |   2023 |          1 | Partidos del GBA | ipc         | 0.940684 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   semestre   14 non-null     int64  
#   1   region     14 non-null     object 
#   2   deflactor  14 non-null     object 
#   3   indice     14 non-null     float64
#  
#  |    |   semestre | region           | deflactor   |   indice |
#  |---:|-----------:|:-----------------|:------------|---------:|
#  |  0 |          1 | Partidos del GBA | ipc         | 0.940684 |
#  
#  ------------------------------
#  
#  drop_col(col='semestre', axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   region     14 non-null     object 
#   1   deflactor  14 non-null     object 
#   2   indice     14 non-null     float64
#  
#  |    | region           | deflactor   |   indice |
#  |---:|:-----------------|:------------|---------:|
#  |  0 | Partidos del GBA | ipc         | 0.940684 |
#  
#  ------------------------------
#  
#  rename_cols(map={'deflactor': 'indicador', 'region': 'categoria', 'indice': 'valor'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   indicador  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |    | categoria        | indicador   |   valor |
#  |---:|:-----------------|:------------|--------:|
#  |  0 | Partidos del GBA | ipc         | 94.0684 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   indicador  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |    | categoria        | indicador   |   valor |
#  |---:|:-----------------|:------------|--------:|
#  |  0 | Partidos del GBA | ipc         | 94.0684 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='ipc', new_value='Ingreso per cápita')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   indicador  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |    | categoria        | indicador          |   valor |
#  |---:|:-----------------|:-------------------|--------:|
#  |  0 | Partidos del GBA | Ingreso per cápita | 94.0684 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='linea de pobreza', new_value='Ingreso per cápita deflactado por línea de pobreza')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   indicador  14 non-null     object 
#   2   valor      14 non-null     float64
#  
#  |    | categoria        | indicador          |   valor |
#  |---:|:-----------------|:-------------------|--------:|
#  |  0 | Partidos del GBA | Ingreso per cápita | 94.0684 |
#  
#  ------------------------------
#  