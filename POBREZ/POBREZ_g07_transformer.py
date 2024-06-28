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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='poverty_line == "pobreza"'),
	replace_value(col='date', curr_value=5, new_value='1'),
	replace_value(col='date', curr_value=10, new_value='2'),
	concatenar_columnas(cols=['year', 'date'], nueva_col='aniosem', separtor='-'),
	drop_col(col=['year', 'date', 'period_type', 'poverty_line', 'fgt_parameter'], axis=1),
	rename_cols(map={'poverty_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 378 entries, 0 to 377
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           378 non-null    int64  
#   1   period_type    378 non-null    object 
#   2   date           378 non-null    int64  
#   3   poverty_line   378 non-null    object 
#   4   fgt_parameter  378 non-null    int64  
#   5   poverty_rate   366 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   fgt_parameter |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|----------------:|---------------:|
#  |  0 |   1992 | mes           |      5 | indigencia     |               0 |        5.37579 |
#  
#  ------------------------------
#  
#  query(condition='poverty_line == "pobreza"')
#  Index: 189 entries, 3 to 377
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           189 non-null    int64  
#   1   period_type    189 non-null    object 
#   2   date           189 non-null    int64  
#   3   poverty_line   189 non-null    object 
#   4   fgt_parameter  189 non-null    int64  
#   5   poverty_rate   183 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   fgt_parameter |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|----------------:|---------------:|
#  |  3 |   1992 | mes           |      5 | pobreza        |               0 |        29.7139 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value=5, new_value='1')
#  Index: 189 entries, 3 to 377
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           189 non-null    int64  
#   1   period_type    189 non-null    object 
#   2   date           189 non-null    object 
#   3   poverty_line   189 non-null    object 
#   4   fgt_parameter  189 non-null    int64  
#   5   poverty_rate   183 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   fgt_parameter |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|----------------:|---------------:|
#  |  3 |   1992 | mes           |      1 | pobreza        |               0 |        29.7139 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value=10, new_value='2')
#  Index: 189 entries, 3 to 377
#  Data columns (total 7 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           189 non-null    int64  
#   1   period_type    189 non-null    object 
#   2   date           189 non-null    object 
#   3   poverty_line   189 non-null    object 
#   4   fgt_parameter  189 non-null    int64  
#   5   poverty_rate   183 non-null    float64
#   6   aniosem        189 non-null    object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   fgt_parameter |   poverty_rate | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|----------------:|---------------:|:----------|
#  |  3 |   1992 | mes           |      1 | pobreza        |               0 |        29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'date'], nueva_col='aniosem', separtor='-')
#  Index: 189 entries, 3 to 377
#  Data columns (total 7 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   year           189 non-null    int64  
#   1   period_type    189 non-null    object 
#   2   date           189 non-null    object 
#   3   poverty_line   189 non-null    object 
#   4   fgt_parameter  189 non-null    int64  
#   5   poverty_rate   183 non-null    float64
#   6   aniosem        189 non-null    object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   fgt_parameter |   poverty_rate | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|----------------:|---------------:|:----------|
#  |  3 |   1992 | mes           |      1 | pobreza        |               0 |        29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'date', 'period_type', 'poverty_line', 'fgt_parameter'], axis=1)
#  Index: 189 entries, 3 to 377
#  Data columns (total 2 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   poverty_rate  183 non-null    float64
#   1   aniosem       189 non-null    object 
#  
#  |    |   poverty_rate | aniosem   |
#  |---:|---------------:|:----------|
#  |  3 |        29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  rename_cols(map={'poverty_rate': 'valor'})
#  Index: 189 entries, 3 to 377
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   valor    183 non-null    float64
#   1   aniosem  189 non-null    object 
#  
#  |    |   valor | aniosem   |
#  |---:|--------:|:----------|
#  |  3 | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  