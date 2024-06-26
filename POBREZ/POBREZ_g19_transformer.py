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
	replace_value(col='age_group', curr_value='0_14_years', new_value='0-14'),
	replace_value(col='age_group', curr_value='15_29_years', new_value='15-29'),
	replace_value(col='age_group', curr_value='30_64_years', new_value='30-64'),
	replace_value(col='age_group', curr_value='65_and_more_years', new_value='65 y más'),
	rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	drop_col(col='poverty_line', axis=1)
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
#  |    |   year | semester   | age_group   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total       | Indigencia     |        22.1105 |
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
#  |    |   year | semester   | age_group   | poverty_line   |   poverty_rate |
#  |---:|-------:|:-----------|:------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Total       | Indigencia     |        22.1105 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='II', new_value=2)
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          400 non-null    int64  
#   1   semester      400 non-null    int64  
#   2   age_group     400 non-null    object 
#   3   poverty_line  400 non-null    object 
#   4   poverty_rate  380 non-null    float64
#   5   aniosem       400 non-null    object 
#  
#  |    |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Total       | Indigencia     |        22.1105 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          400 non-null    int64  
#   1   semester      400 non-null    int64  
#   2   age_group     400 non-null    object 
#   3   poverty_line  400 non-null    object 
#   4   poverty_rate  380 non-null    float64
#   5   aniosem       400 non-null    object 
#  
#  |    |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Total       | Indigencia     |        22.1105 | 2003-2    |
#  
#  ------------------------------
#  
#  query(condition="poverty_line == 'Pobreza'")
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   age_group     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   poverty_rate  190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        |        59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  replace_value(col='age_group', curr_value='0_14_years', new_value='0-14')
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   age_group     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   poverty_rate  190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        |        59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  replace_value(col='age_group', curr_value='15_29_years', new_value='15-29')
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   age_group     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   poverty_rate  190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        |        59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  replace_value(col='age_group', curr_value='30_64_years', new_value='30-64')
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   age_group     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   poverty_rate  190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        |        59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  replace_value(col='age_group', curr_value='65_and_more_years', new_value='65 y más')
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   age_group     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   poverty_rate  190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | age_group   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|---------------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        |        59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'})
#  Index: 200 entries, 200 to 399
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          200 non-null    int64  
#   1   semester      200 non-null    int64  
#   2   categoria     200 non-null    object 
#   3   poverty_line  200 non-null    object 
#   4   valor         190 non-null    float64
#   5   aniosem       200 non-null    object 
#  
#  |     |   year |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|--------:|:----------|
#  | 200 |   2003 |          2 | Total       | Pobreza        | 59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 200 entries, 200 to 399
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   semester      200 non-null    int64  
#   1   categoria     200 non-null    object 
#   2   poverty_line  200 non-null    object 
#   3   valor         190 non-null    float64
#   4   aniosem       200 non-null    object 
#  
#  |     |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-----------:|:------------|:---------------|--------:|:----------|
#  | 200 |          2 | Total       | Pobreza        | 59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 200 entries, 200 to 399
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     200 non-null    object 
#   1   poverty_line  200 non-null    object 
#   2   valor         190 non-null    float64
#   3   aniosem       200 non-null    object 
#  
#  |     | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|:------------|:---------------|--------:|:----------|
#  | 200 | Total       | Pobreza        | 59.9173 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 200 entries, 200 to 399
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  200 non-null    object 
#   1   valor      190 non-null    float64
#   2   aniosem    200 non-null    object 
#  
#  |     | categoria   |   valor | aniosem   |
#  |----:|:------------|--------:|:----------|
#  | 200 | Total       | 59.9173 | 2003-2    |
#  
#  ------------------------------
#  