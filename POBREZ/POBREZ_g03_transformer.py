from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="region == 'Total'"),
	replace_value(col='semester', curr_value='I', new_value=1),
	replace_value(col='semester', curr_value='II', new_value=2),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	rename_cols(map={'k_value': 'categoria', 'pov_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	drop_col(col='region', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 640 entries, 0 to 639
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      640 non-null    int64  
#   1   semester  640 non-null    object 
#   2   region    640 non-null    object 
#   3   k_value   640 non-null    float64
#   4   pov_rate  608 non-null    float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  query(condition="region == 'Total'")
#  Index: 80 entries, 0 to 632
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      80 non-null     int64  
#   1   semester  80 non-null     object 
#   2   region    80 non-null     object 
#   3   k_value   80 non-null     float64
#   4   pov_rate  76 non-null     float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value=1)
#  Index: 80 entries, 0 to 632
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      80 non-null     int64  
#   1   semester  80 non-null     object 
#   2   region    80 non-null     object 
#   3   k_value   80 non-null     float64
#   4   pov_rate  76 non-null     float64
#  
#  |    |   year | semester   | region   |   k_value |   pov_rate |
#  |---:|-------:|:-----------|:---------|----------:|-----------:|
#  |  0 |   2003 | II         | Total    |      0.25 |   0.256201 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='II', new_value=2)
#  Index: 80 entries, 0 to 632
#  Data columns (total 6 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      80 non-null     int64  
#   1   semester  80 non-null     int64  
#   2   region    80 non-null     object 
#   3   k_value   80 non-null     float64
#   4   pov_rate  76 non-null     float64
#   5   aniosem   80 non-null     object 
#  
#  |    |   year |   semester | region   |   k_value |   pov_rate | aniosem   |
#  |---:|-------:|-----------:|:---------|----------:|-----------:|:----------|
#  |  0 |   2003 |          2 | Total    |      0.25 |   0.256201 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  Index: 80 entries, 0 to 632
#  Data columns (total 6 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      80 non-null     int64  
#   1   semester  80 non-null     int64  
#   2   region    80 non-null     object 
#   3   k_value   80 non-null     float64
#   4   pov_rate  76 non-null     float64
#   5   aniosem   80 non-null     object 
#  
#  |    |   year |   semester | region   |   k_value |   pov_rate | aniosem   |
#  |---:|-------:|-----------:|:---------|----------:|-----------:|:----------|
#  |  0 |   2003 |          2 | Total    |      0.25 |   0.256201 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'k_value': 'categoria', 'pov_rate': 'valor'})
#  Index: 80 entries, 0 to 632
#  Data columns (total 6 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       80 non-null     int64  
#   1   semester   80 non-null     int64  
#   2   region     80 non-null     object 
#   3   categoria  80 non-null     float64
#   4   valor      76 non-null     float64
#   5   aniosem    80 non-null     object 
#  
#  |    |   year |   semester | region   |   categoria |    valor | aniosem   |
#  |---:|-------:|-----------:|:---------|------------:|---------:|:----------|
#  |  0 |   2003 |          2 | Total    |        0.25 | 0.256201 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 80 entries, 0 to 632
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   semester   80 non-null     int64  
#   1   region     80 non-null     object 
#   2   categoria  80 non-null     float64
#   3   valor      76 non-null     float64
#   4   aniosem    80 non-null     object 
#  
#  |    |   semester | region   |   categoria |    valor | aniosem   |
#  |---:|-----------:|:---------|------------:|---------:|:----------|
#  |  0 |          2 | Total    |        0.25 | 0.256201 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 80 entries, 0 to 632
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   region     80 non-null     object 
#   1   categoria  80 non-null     float64
#   2   valor      76 non-null     float64
#   3   aniosem    80 non-null     object 
#  
#  |    | region   |   categoria |    valor | aniosem   |
#  |---:|:---------|------------:|---------:|:----------|
#  |  0 | Total    |        0.25 | 0.256201 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='region', axis=1)
#  Index: 80 entries, 0 to 632
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  80 non-null     float64
#   1   valor      76 non-null     float64
#   2   aniosem    80 non-null     object 
#  
#  |    |   categoria |    valor | aniosem   |
#  |---:|------------:|---------:|:----------|
#  |  0 |        0.25 | 0.256201 | 2003-2    |
#  
#  ------------------------------
#  