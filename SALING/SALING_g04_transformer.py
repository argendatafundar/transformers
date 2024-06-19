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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='year', axis=1),
	drop_col(col='semestre', axis=1),
	rename_cols(map={'deflactor': 'indicador', 'region': 'categoria', 'indice': 'valor'})
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
#  |    | categoria        | indicador   |    valor |
#  |---:|:-----------------|:------------|---------:|
#  |  0 | Partidos del GBA | ipc         | 0.940684 |
#  
#  ------------------------------
#  