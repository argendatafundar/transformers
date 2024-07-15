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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	rename_cols(map={'country_code': 'geocodigo', 'poverty_rate': 'valor'}),
	get_anios_mas_cercanos(group_col='geocodigo', anio_col='year', anio=2019, thresh=3),
	mutiplicar_por_escalar(col='valor', k=100)
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
#  query(condition='poverty_line == 6.85')
#  Index: 109 entries, 218 to 326
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  109 non-null    object 
#   1   year          109 non-null    int64  
#   2   poverty_line  109 non-null    float64
#   3   poverty_rate  109 non-null    float64
#  
#  |     | country_code   |   year |   poverty_line |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|---------------:|
#  | 218 | AGO            |   2018 |           6.85 |       0.779747 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  109 non-null    object 
#   1   year          109 non-null    int64  
#   2   poverty_rate  109 non-null    float64
#  
#  |     | country_code   |   year |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|
#  | 218 | AGO            |   2018 |       0.779747 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'geocodigo', 'poverty_rate': 'valor'})
#  Index: 109 entries, 218 to 326
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geocodigo    109 non-null    object 
#   1   year         109 non-null    int64  
#   2   valor        109 non-null    float64
#   3   bool_max     109 non-null    bool   
#   4   bool_select  109 non-null    bool   
#  
#  |     | geocodigo   |   year |    valor | bool_max   | bool_select   |
#  |----:|:------------|-------:|---------:|:-----------|:--------------|
#  | 218 | AGO         |   2018 | 0.779747 | True       | True          |
#  
#  ------------------------------
#  
#  get_anios_mas_cercanos(group_col='geocodigo', anio_col='year', anio=2019, thresh=3)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | geocodigo   |   year |   valor |
#  |----:|:------------|-------:|--------:|
#  | 218 | AGO         |   2018 | 77.9747 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | geocodigo   |   year |   valor |
#  |----:|:------------|-------:|--------:|
#  | 218 | AGO         |   2018 | 77.9747 |
#  
#  ------------------------------
#  