from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
query(condition='year == year.max()'),
	query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	drop_col(col='year', axis=1),
	rename_cols(map={'country_code': 'geocodigo', 'poverty_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 545 entries, 0 to 544
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  545 non-null    object 
#   1   year          545 non-null    int64  
#   2   poverty_line  545 non-null    float64
#   3   poverty_rate  545 non-null    float64
#  
#  |    | country_code   |   year |   poverty_line |   poverty_rate |
#  |---:|:---------------|-------:|---------------:|---------------:|
#  |  0 | AGO            |   2018 |           2.15 |        0.31122 |
#  
#  ------------------------------
#  
#  query(condition='year == year.max()')
#  Index: 355 entries, 1 to 544
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  355 non-null    object 
#   1   year          355 non-null    int64  
#   2   poverty_line  355 non-null    float64
#   3   poverty_rate  355 non-null    float64
#  
#  |    | country_code   |   year |   poverty_line |   poverty_rate |
#  |---:|:---------------|-------:|---------------:|---------------:|
#  |  1 | ALB            |   2019 |           2.15 |              0 |
#  
#  ------------------------------
#  
#  query(condition='poverty_line == 6.85')
#  Index: 71 entries, 219 to 326
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  71 non-null     object 
#   1   year          71 non-null     int64  
#   2   poverty_line  71 non-null     float64
#   3   poverty_rate  71 non-null     float64
#  
#  |     | country_code   |   year |   poverty_line |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|---------------:|
#  | 219 | ALB            |   2019 |           6.85 |       0.149019 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 71 entries, 219 to 326
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  71 non-null     object 
#   1   year          71 non-null     int64  
#   2   poverty_rate  71 non-null     float64
#  
#  |     | country_code   |   year |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|
#  | 219 | ALB            |   2019 |       0.149019 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 71 entries, 219 to 326
#  Data columns (total 2 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  71 non-null     object 
#   1   poverty_rate  71 non-null     float64
#  
#  |     | country_code   |   poverty_rate |
#  |----:|:---------------|---------------:|
#  | 219 | ALB            |       0.149019 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'geocodigo', 'poverty_rate': 'valor'})
#  Index: 71 entries, 219 to 326
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  71 non-null     object 
#   1   valor      71 non-null     float64
#  
#  |     | geocodigo   |    valor |
#  |----:|:------------|---------:|
#  | 219 | ALB         | 0.149019 |
#  
#  ------------------------------
#  