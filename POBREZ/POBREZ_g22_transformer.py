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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='semester', curr_value='II', new_value='2'),
	replace_value(col='semester', curr_value='I', new_value='1'),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	query(condition="pov_type == 'difference' "),
	drop_col(col=['year', 'semester', 'pov_type'], axis=1),
	rename_cols(map={'poverty_line': 'categoria', 'poverty_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   poverty_line  240 non-null    object 
#   3   pov_type      240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year | semester   | poverty_line   | pov_type       |   poverty_rate |
#  |---:|-------:|:-----------|:---------------|:---------------|---------------:|
#  |  0 |   2003 | II         | Indigencia     | with_transfers |        32.0614 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='II', new_value='2')
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   poverty_line  240 non-null    object 
#   3   pov_type      240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#  
#  |    |   year |   semester | poverty_line   | pov_type       |   poverty_rate |
#  |---:|-------:|-----------:|:---------------|:---------------|---------------:|
#  |  0 |   2003 |          2 | Indigencia     | with_transfers |        32.0614 |
#  
#  ------------------------------
#  
#  replace_value(col='semester', curr_value='I', new_value='1')
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   poverty_line  240 non-null    object 
#   3   pov_type      240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |    |   year |   semester | poverty_line   | pov_type       |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:---------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Indigencia     | with_transfers |        32.0614 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          240 non-null    int64  
#   1   semester      240 non-null    object 
#   2   poverty_line  240 non-null    object 
#   3   pov_type      240 non-null    object 
#   4   poverty_rate  228 non-null    float64
#   5   aniosem       240 non-null    object 
#  
#  |    |   year |   semester | poverty_line   | pov_type       |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:---------------|:---------------|---------------:|:----------|
#  |  0 |   2003 |          2 | Indigencia     | with_transfers |        32.0614 | 2003-2    |
#  
#  ------------------------------
#  
#  query(condition="pov_type == 'difference' ")
#  Index: 80 entries, 2 to 239
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   year          80 non-null     int64  
#   1   semester      80 non-null     object 
#   2   poverty_line  80 non-null     object 
#   3   pov_type      80 non-null     object 
#   4   poverty_rate  76 non-null     float64
#   5   aniosem       80 non-null     object 
#  
#  |    |   year |   semester | poverty_line   | pov_type   |   poverty_rate | aniosem   |
#  |---:|-------:|-----------:|:---------------|:-----------|---------------:|:----------|
#  |  2 |   2003 |          2 | Indigencia     | difference |        2.61839 | 2003-2    |
#  
#  ------------------------------
#  
#  drop_col(col=['year', 'semester', 'pov_type'], axis=1)
#  Index: 80 entries, 2 to 239
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   poverty_line  80 non-null     object 
#   1   poverty_rate  76 non-null     float64
#   2   aniosem       80 non-null     object 
#  
#  |    | poverty_line   |   poverty_rate | aniosem   |
#  |---:|:---------------|---------------:|:----------|
#  |  2 | Indigencia     |        2.61839 | 2003-2    |
#  
#  ------------------------------
#  
#  rename_cols(map={'poverty_line': 'categoria', 'poverty_rate': 'valor'})
#  Index: 80 entries, 2 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  80 non-null     object 
#   1   valor      76 non-null     float64
#   2   aniosem    80 non-null     object 
#  
#  |    | categoria   |   valor | aniosem   |
#  |---:|:------------|--------:|:----------|
#  |  2 | Indigencia  | 2.61839 | 2003-2    |
#  
#  ------------------------------
#  