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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="(region_name == 'Argentina') | (region_name == 'Latin America and Caribbean')"),
	query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	drop_col(col='region_code', axis=1),
	replace_value(col='region_name', curr_value='Latin America and Caribbean', new_value='América Latina y el Caribe (excluidos Países de Altos Ingresos)'),
	rename_cols(map={'region_name': 'categoria', 'poverty_rate': 'valor', 'year': 'anio'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1915 entries, 0 to 1914
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_code   1915 non-null   object 
#   1   region_name   1915 non-null   object 
#   2   year          1915 non-null   int64  
#   3   poverty_line  1915 non-null   float64
#   4   poverty_rate  1915 non-null   float64
#  
#  |    | region_code   | region_name                 |   year |   poverty_line |   poverty_rate |
#  |---:|:--------------|:----------------------------|-------:|---------------:|---------------:|
#  |  0 | AFE           | Eastern and Southern Africa |   1993 |           2.15 |       0.584803 |
#  
#  ------------------------------
#  
#  query(condition="(region_name == 'Argentina') | (region_name == 'Latin America and Caribbean')")
#  Index: 370 entries, 60 to 1736
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_code   370 non-null    object 
#   1   region_name   370 non-null    object 
#   2   year          370 non-null    int64  
#   3   poverty_line  370 non-null    float64
#   4   poverty_rate  370 non-null    float64
#  
#  |    | region_code   | region_name   |   year |   poverty_line |   poverty_rate |
#  |---:|:--------------|:--------------|-------:|---------------:|---------------:|
#  | 60 | ARG           | Argentina     |   1980 |           2.15 |     0.00309012 |
#  
#  ------------------------------
#  
#  query(condition='poverty_line == 6.85')
#  Index: 74 entries, 826 to 970
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_code   74 non-null     object 
#   1   region_name   74 non-null     object 
#   2   year          74 non-null     int64  
#   3   poverty_line  74 non-null     float64
#   4   poverty_rate  74 non-null     float64
#  
#  |     | region_code   | region_name   |   year |   poverty_line |   poverty_rate |
#  |----:|:--------------|:--------------|-------:|---------------:|---------------:|
#  | 826 | ARG           | Argentina     |   1980 |           6.85 |      0.0552647 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 74 entries, 826 to 970
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_code   74 non-null     object 
#   1   region_name   74 non-null     object 
#   2   year          74 non-null     int64  
#   3   poverty_rate  74 non-null     float64
#  
#  |     | region_code   | region_name   |   year |   poverty_rate |
#  |----:|:--------------|:--------------|-------:|---------------:|
#  | 826 | ARG           | Argentina     |   1980 |      0.0552647 |
#  
#  ------------------------------
#  
#  drop_col(col='region_code', axis=1)
#  Index: 74 entries, 826 to 970
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_name   74 non-null     object 
#   1   year          74 non-null     int64  
#   2   poverty_rate  74 non-null     float64
#  
#  |     | region_name   |   year |   poverty_rate |
#  |----:|:--------------|-------:|---------------:|
#  | 826 | Argentina     |   1980 |      0.0552647 |
#  
#  ------------------------------
#  
#  replace_value(col='region_name', curr_value='Latin America and Caribbean', new_value='América Latina y el Caribe (excluidos Países de Altos Ingresos)')
#  Index: 74 entries, 826 to 970
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_name   74 non-null     object 
#   1   year          74 non-null     int64  
#   2   poverty_rate  74 non-null     float64
#  
#  |     | region_name   |   year |   poverty_rate |
#  |----:|:--------------|-------:|---------------:|
#  | 826 | Argentina     |   1980 |      0.0552647 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region_name': 'categoria', 'poverty_rate': 'valor', 'year': 'anio'})
#  Index: 74 entries, 826 to 970
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  74 non-null     object 
#   1   anio       74 non-null     int64  
#   2   valor      74 non-null     float64
#  
#  |     | categoria   |   anio |     valor |
#  |----:|:------------|-------:|----------:|
#  | 826 | Argentina   |   1980 | 0.0552647 |
#  
#  ------------------------------
#  