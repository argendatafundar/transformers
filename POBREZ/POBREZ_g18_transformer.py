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
	rename_cols(map={'educ_level': 'categoria', 'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='semester', axis=1),
	drop_col(col='poverty_line', axis=1)
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
#  replace_value(col='semester', curr_value='II', new_value=2)
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          480 non-null    int64  
#   1   semester      480 non-null    int64  
#   2   educ_level    480 non-null    object 
#   3   poverty_line  480 non-null    object 
#   4   poverty_rate  456 non-null    float64
#   5   aniosem       480 non-null    object 
#  
#  |    |   year |   semester | educ_level   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:-------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Todos        | Indigencia     |        18.3672 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 480 entries, 0 to 479
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          480 non-null    int64  
#   1   semester      480 non-null    int64  
#   2   educ_level    480 non-null    object 
#   3   poverty_line  480 non-null    object 
#   4   poverty_rate  456 non-null    float64
#   5   aniosem       480 non-null    object 
#  
#  |    |   year |   semester | educ_level   | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:-------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Todos        | Indigencia     |        18.3672 | 2003-2    |
#  
#  ------------------------------
#  
#  query(condition="poverty_line == 'Pobreza'")
#  Index: 240 entries, 240 to 479
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    int64  
#   2   educ_level    240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |     |   year |   semester | educ_level   | poverty_line   |   poverty_rate | aniosem   |
#  |----:|-------:|-----------:|:-------------|:---------------|---------------:|:----------|
#  | 240 |   2003 |          2 | Todos        | Pobreza        |        54.6942 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'educ_level': 'categoria', 'poverty_rate': 'valor'})
#  Index: 240 entries, 240 to 479
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    int64  
#   2   categoria     240 non-null    object 
#   3   poverty_line  240 non-null    object 
#   4   valor         228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |     |   year |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-------:|-----------:|:------------|:---------------|--------:|:----------|
#  | 240 |   2003 |          2 | Todos       | Pobreza        | 54.6942 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 240 entries, 240 to 479
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   semester      240 non-null    int64  
#   1   categoria     240 non-null    object 
#   2   poverty_line  240 non-null    object 
#   3   valor         228 non-null    float64
#   4   aniosem       240 non-null    object 
#  
#  |     |   semester | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|-----------:|:------------|:---------------|--------:|:----------|
#  | 240 |          2 | Todos       | Pobreza        | 54.6942 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='semester', axis=1)
#  Index: 240 entries, 240 to 479
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     240 non-null    object 
#   1   poverty_line  240 non-null    object 
#   2   valor         228 non-null    float64
#   3   aniosem       240 non-null    object 
#  
#  |     | categoria   | poverty_line   |   valor | aniosem   |
#  |----:|:------------|:---------------|--------:|:----------|
#  | 240 | Todos       | Pobreza        | 54.6942 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 240 entries, 240 to 479
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  240 non-null    object 
#   1   valor      228 non-null    float64
#   2   aniosem    240 non-null    object 
#  
#  |     | categoria   |   valor | aniosem   |
#  |----:|:------------|--------:|:----------|
#  | 240 | Todos       | 54.6942 | 2003-2    |
#  
#  ------------------------------
#  