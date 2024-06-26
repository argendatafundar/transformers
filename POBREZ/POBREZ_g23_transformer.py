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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='date', curr_value=5, new_value=1),
	replace_value(col='date', curr_value=10, new_value=2),
	replace_value(col='date', curr_value='I', new_value=1),
	replace_value(col='date', curr_value='II', new_value=2),
	concatenar_columnas(cols=['year', 'date'], nueva_col='aniosem', separtor='-'),
	query(condition="poverty_line == 'pobreza'"),
	rename_cols(map={'poverty_rate': 'valor'}),
	drop_col(col='year', axis=1),
	drop_col(col='period_type', axis=1),
	drop_col(col='date', axis=1),
	drop_col(col='poverty_line', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|
#  |  0 |   1992 | mes           |      5 | indigencia     |        5.37579 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value=5, new_value=1)
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|
#  |  0 |   1992 | mes           |      1 | indigencia     |        5.37579 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value=10, new_value=2)
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|
#  |  0 |   1992 | mes           |      1 | indigencia     |        5.37579 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='I', new_value=1)
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|
#  |  0 |   1992 | mes           |      1 | indigencia     |        5.37579 |
#  
#  ------------------------------
#  
#  replace_value(col='date', curr_value='II', new_value=2)
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#   5   aniosem       128 non-null    object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|:----------|
#  |  0 |   1992 | mes           |      1 | indigencia     |        5.37579 | 1992-1    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'date'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          128 non-null    int64  
#   1   period_type   128 non-null    object 
#   2   date          128 non-null    int64  
#   3   poverty_line  128 non-null    object 
#   4   poverty_rate  124 non-null    float64
#   5   aniosem       128 non-null    object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|:----------|
#  |  0 |   1992 | mes           |      1 | indigencia     |        5.37579 | 1992-1    |
#  
#  ------------------------------
#  
#  query(condition="poverty_line == 'pobreza'")
#  Index: 64 entries, 1 to 127
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          64 non-null     int64  
#   1   period_type   64 non-null     object 
#   2   date          64 non-null     int64  
#   3   poverty_line  64 non-null     object 
#   4   poverty_rate  62 non-null     float64
#   5   aniosem       64 non-null     object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   poverty_rate | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|---------------:|:----------|
#  |  1 |   1992 | mes           |      1 | pobreza        |        29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  rename_cols(map={'poverty_rate': 'valor'})
#  Index: 64 entries, 1 to 127
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          64 non-null     int64  
#   1   period_type   64 non-null     object 
#   2   date          64 non-null     int64  
#   3   poverty_line  64 non-null     object 
#   4   valor         62 non-null     float64
#   5   aniosem       64 non-null     object 
#  
#  |    |   year | period_type   |   date | poverty_line   |   valor | aniosem   |
#  |---:|-------:|:--------------|-------:|:---------------|--------:|:----------|
#  |  1 |   1992 | mes           |      1 | pobreza        | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 64 entries, 1 to 127
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   period_type   64 non-null     object 
#   1   date          64 non-null     int64  
#   2   poverty_line  64 non-null     object 
#   3   valor         62 non-null     float64
#   4   aniosem       64 non-null     object 
#  
#  |    | period_type   |   date | poverty_line   |   valor | aniosem   |
#  |---:|:--------------|-------:|:---------------|--------:|:----------|
#  |  1 | mes           |      1 | pobreza        | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  drop_col(col='period_type', axis=1)
#  Index: 64 entries, 1 to 127
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   date          64 non-null     int64  
#   1   poverty_line  64 non-null     object 
#   2   valor         62 non-null     float64
#   3   aniosem       64 non-null     object 
#  
#  |    |   date | poverty_line   |   valor | aniosem   |
#  |---:|-------:|:---------------|--------:|:----------|
#  |  1 |      1 | pobreza        | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  drop_col(col='date', axis=1)
#  Index: 64 entries, 1 to 127
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   poverty_line  64 non-null     object 
#   1   valor         62 non-null     float64
#   2   aniosem       64 non-null     object 
#  
#  |    | poverty_line   |   valor | aniosem   |
#  |---:|:---------------|--------:|:----------|
#  |  1 | pobreza        | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 64 entries, 1 to 127
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   valor    62 non-null     float64
#   1   aniosem  64 non-null     object 
#  
#  |    |   valor | aniosem   |
#  |---:|--------:|:----------|
#  |  1 | 29.7139 | 1992-1    |
#  
#  ------------------------------
#  