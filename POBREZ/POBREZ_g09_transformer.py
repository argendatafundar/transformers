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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='year == 2021'),
	query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	drop_col(col='year', axis=1),
	rename_cols(map={'country_code': 'categoria', 'poverty_rate': 'valor'}),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='BOL', new_value='Bolivia'),
	replace_value(col='categoria', curr_value='BRA', new_value='Brasil'),
	replace_value(col='categoria', curr_value='COL', new_value='Colombia'),
	replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica'),
	replace_value(col='categoria', curr_value='DOM', new_value='Dominicana'),
	replace_value(col='categoria', curr_value='ECU', new_value='Ecuador'),
	replace_value(col='categoria', curr_value='PAN', new_value='Panamá'),
	replace_value(col='categoria', curr_value='PER', new_value='Perú'),
	replace_value(col='categoria', curr_value='PRY', new_value='Paraguay'),
	replace_value(col='categoria', curr_value='SLV', new_value='El Salvador'),
	replace_value(col='categoria', curr_value='URY', new_value='Uruguay'),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2250 entries, 0 to 2249
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  2250 non-null   object 
#   1   year          2250 non-null   int64  
#   2   poverty_line  2250 non-null   float64
#   3   poverty_rate  2250 non-null   float64
#  
#  |    | country_code   |   year |   poverty_line |   poverty_rate |
#  |---:|:---------------|-------:|---------------:|---------------:|
#  |  0 | ARG            |   1980 |           2.15 |     0.00309012 |
#  
#  ------------------------------
#  
#  query(condition='year == 2021')
#  Index: 60 entries, 32 to 2236
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  60 non-null     object 
#   1   year          60 non-null     int64  
#   2   poverty_line  60 non-null     float64
#   3   poverty_rate  60 non-null     float64
#  
#  |    | country_code   |   year |   poverty_line |   poverty_rate |
#  |---:|:---------------|-------:|---------------:|---------------:|
#  | 32 | ARG            |   2021 |           2.15 |     0.00958847 |
#  
#  ------------------------------
#  
#  query(condition='poverty_line == 6.85')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  12 non-null     object 
#   1   year          12 non-null     int64  
#   2   poverty_line  12 non-null     float64
#   3   poverty_rate  12 non-null     float64
#  
#  |     | country_code   |   year |   poverty_line |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|---------------:|
#  | 932 | ARG            |   2021 |           6.85 |       0.106201 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 12 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  12 non-null     object 
#   1   year          12 non-null     int64  
#   2   poverty_rate  12 non-null     float64
#  
#  |     | country_code   |   year |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|
#  | 932 | ARG            |   2021 |       0.106201 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  12 non-null     object 
#   1   poverty_rate  12 non-null     float64
#  
#  |     | country_code   |   poverty_rate |
#  |----:|:---------------|---------------:|
#  | 932 | ARG            |       0.106201 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'categoria', 'poverty_rate': 'valor'})
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | ARG         | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BOL', new_value='Bolivia')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BRA', new_value='Brasil')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='COL', new_value='Colombia')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DOM', new_value='Dominicana')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ECU', new_value='Ecuador')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAN', new_value='Panamá')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PER', new_value='Perú')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRY', new_value='Paraguay')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLV', new_value='El Salvador')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |    valor |
#  |----:|:------------|---------:|
#  | 932 | Argentina   | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |   valor |
#  |----:|:------------|--------:|
#  | 932 | Argentina   | 10.6201 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 12 entries, 932 to 1336
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |     | categoria   |   valor |
#  |----:|:------------|--------:|
#  | 932 | Argentina   | 10.6201 |
#  
#  ------------------------------
#  