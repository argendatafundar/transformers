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
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='semester', curr_value='I', new_value=1),
	replace_value(col='semester', curr_value='II', new_value=2),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	query(condition="poverty_line == 'Pobreza'"),
	rename_cols(map={'gender': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	drop_col(col='poverty_line', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   gender        240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | gender   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:---------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total    | Indigencia     |         22.121 |
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
#   2   gender        240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | gender   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:---------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total    | Indigencia     |         22.121 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='II', new_value=2)
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    int64  
#   2   gender        240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |    |   year |   semester | gender   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:---------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Total    | Indigencia     |         22.121 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    int64  
#   2   gender        240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |    |   year |   semester | gender   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:---------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Total    | Indigencia     |         22.121 | 2003-2    |
#  
#  ------------------------------
#  
#  query(condition="poverty_line == 'Pobreza'")
#  Index: 120 entries, 120 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          120 non-null    int64  
#   1   semester      120 non-null    int64  
#   2   gender        120 non-null    object 
#   3   poverty_line  120 non-null    object 
#   4   poverty_rate  114 non-null    float64
#   5   aniosem       120 non-null    object 
#  
#  |     |   year |   semester | gender   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:---------|:---------------|---------------:|:----------|
#  | 120 |   2003 |          2 | Total    | Pobreza        |        59.9318 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'gender': 'categoria', 'poverty_rate': 'valor'})
#  Index: 120 entries, 120 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          120 non-null    int64  
#   1   semester      120 non-null    int64  
#   2   categoria     120 non-null    object 
#   3   poverty_line  120 non-null    object 
#   4   valor         114 non-null    float64
#   5   aniosem       120 non-null    object 
#  
#  |     |   year |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|--------:|:----------|
#  | 120 |   2003 |          2 | Total       | Pobreza        | 59.9318 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 120 entries, 120 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   semester      120 non-null    int64  
#   1   categoria     120 non-null    object 
#   2   poverty_line  120 non-null    object 
#   3   valor         114 non-null    float64
#   4   aniosem       120 non-null    object 
#  
#  |     |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-----------:|:------------|:---------------|--------:|:----------|
#  | 120 |          2 | Total       | Pobreza        | 59.9318 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 120 entries, 120 to 239
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     120 non-null    object 
#   1   poverty_line  120 non-null    object 
#   2   valor         114 non-null    float64
#   3   aniosem       120 non-null    object 
#  
#  |     | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|:------------|:---------------|--------:|:----------|
#  | 120 | Total       | Pobreza        | 59.9318 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 120 entries, 120 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  120 non-null    object 
#   1   valor      114 non-null    float64
#   2   aniosem    120 non-null    object 
#  
#  |     | categoria   |   valor | aniosem   |
#  |----:|:------------|--------:|:----------|
#  | 120 | Total       | 59.9318 | 2003-2    |
#  
#  ------------------------------
#  