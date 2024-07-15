from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def get_anios_mas_cercanos(df:DataFrame, group_col:str, anio_col:str = 'anio', anio:int = 2021, thresh:int = 3): 
    
    # lambda serie: serie.max() == maximo
    def es_anio_max(serie):
        maximo = serie.max()
        return serie == maximo

    df['bool_max'] = df.groupby(group_col)[anio_col].transform(es_anio_max)
    df['bool_select'] = abs(df[anio_col] - anio) <= thresh

    df = df.loc[df.bool_max & df.bool_select,:].drop(columns=['bool_max','bool_select'])
    return df

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

@transformer.convert
def concatenar_anio_a_pais(df:DataFrame, col_pais:str, col_anio): 
    df[col_pais] = df[col_pais].astype(str) + " (" + df[col_anio].astype(str) + ")"
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	get_anios_mas_cercanos(group_col='country_code', anio_col='year', anio=2021, thresh=3),
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
	replace_value(col='categoria', curr_value='CHL', new_value='Chile'),
	replace_value(col='catgegoria', curr_value='HND', new_value='Honduras'),
	replace_value(col='catgegoria', curr_value='MEX', new_value='México'),
	mutiplicar_por_escalar(col='valor', k=100),
	concatenar_anio_a_pais(col_pais='categoria', col_anio='year')
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
#  query(condition='poverty_line == 6.85')
#  Index: 450 entries, 900 to 1349
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  450 non-null    object 
#   1   year          450 non-null    int64  
#   2   poverty_line  450 non-null    float64
#   3   poverty_rate  450 non-null    float64
#  
#  |     | country_code   |   year |   poverty_line |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|---------------:|
#  | 900 | ARG            |   1980 |           6.85 |      0.0552647 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 450 entries, 900 to 1349
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  450 non-null    object 
#   1   year          450 non-null    int64  
#   2   poverty_rate  450 non-null    float64
#   3   bool_max      450 non-null    bool   
#   4   bool_select   450 non-null    bool   
#  
#  |     | country_code   |   year |   poverty_rate | bool_max   | bool_select   |
#  |----:|:---------------|-------:|---------------:|:-----------|:--------------|
#  | 900 | ARG            |   1980 |      0.0552647 | False      | False         |
#  
#  ------------------------------
#  
#  get_anios_mas_cercanos(group_col='country_code', anio_col='year', anio=2021, thresh=3)
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  15 non-null     object 
#   1   year          15 non-null     int64  
#   2   poverty_rate  15 non-null     float64
#  
#  |     | country_code   |   year |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|
#  | 932 | ARG            |   2021 |       0.106201 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'categoria', 'poverty_rate': 'valor'})
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | ARG         |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BOL', new_value='Bolivia')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BRA', new_value='Brasil')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='COL', new_value='Colombia')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DOM', new_value='Dominicana')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ECU', new_value='Ecuador')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAN', new_value='Panamá')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PER', new_value='Perú')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRY', new_value='Paraguay')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLV', new_value='El Salvador')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHL', new_value='Chile')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='catgegoria', curr_value='HND', new_value='Honduras')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 932 | Argentina   |   2021 | 0.106201 |
#  
#  ------------------------------
#  
#  replace_value(col='catgegoria', curr_value='MEX', new_value='México')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria        |   year |   valor |
#  |----:|:-----------------|-------:|--------:|
#  | 932 | Argentina (2021) |   2021 | 10.6201 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria        |   year |   valor |
#  |----:|:-----------------|-------:|--------:|
#  | 932 | Argentina (2021) |   2021 | 10.6201 |
#  
#  ------------------------------
#  
#  concatenar_anio_a_pais(col_pais='categoria', col_anio='year')
#  Index: 15 entries, 932 to 1336
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   year       15 non-null     int64  
#   2   valor      15 non-null     float64
#  
#  |     | categoria        |   year |   valor |
#  |----:|:-----------------|-------:|--------:|
#  | 932 | Argentina (2021) |   2021 | 10.6201 |
#  
#  ------------------------------
#  