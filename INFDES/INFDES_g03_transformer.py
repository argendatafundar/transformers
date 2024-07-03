from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
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
latest_year(by='anio'),
	rename_cols(map={'iso3': 'geocodigo', 'cat_ocup_detalle': 'indicador'}),
	drop_col(col='formal_def_productiva', axis=1),
	drop_col(col='cat_ocup_cod', axis=1),
	drop_col(col='pais', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 120 entries, 0 to 119
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   iso3                   120 non-null    object 
#   1   pais                   120 non-null    object 
#   2   anio                   120 non-null    int64  
#   3   formal_def_productiva  120 non-null    object 
#   4   cat_ocup_cod           105 non-null    float64
#   5   cat_ocup_detalle       120 non-null    object 
#   6   valor                  120 non-null    float64
#  
#  |    | iso3   | pais      |   anio | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:-------|:----------|-------:|:------------------------|---------------:|:-------------------|--------:|
#  |  0 | ARG    | Argentina |   2022 | Formal                  |              1 | Empleadores        |     3.7 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 80 entries, 0 to 119
#  Data columns (total 6 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   iso3                   80 non-null     object 
#   1   pais                   80 non-null     object 
#   2   formal_def_productiva  80 non-null     object 
#   3   cat_ocup_cod           70 non-null     float64
#   4   cat_ocup_detalle       80 non-null     object 
#   5   valor                  80 non-null     float64
#  
#  |    | iso3   | pais      | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:-------|:----------|:------------------------|---------------:|:-------------------|--------:|
#  |  0 | ARG    | Argentina | Formal                  |              1 | Empleadores        |     3.7 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cat_ocup_detalle': 'indicador'})
#  Index: 80 entries, 0 to 119
#  Data columns (total 6 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigo              80 non-null     object 
#   1   pais                   80 non-null     object 
#   2   formal_def_productiva  80 non-null     object 
#   3   cat_ocup_cod           70 non-null     float64
#   4   indicador              80 non-null     object 
#   5   valor                  80 non-null     float64
#  
#  |    | geocodigo   | pais      | formal_def_productiva   |   cat_ocup_cod | indicador   |   valor |
#  |---:|:------------|:----------|:------------------------|---------------:|:------------|--------:|
#  |  0 | ARG         | Argentina | Formal                  |              1 | Empleadores |     3.7 |
#  
#  ------------------------------
#  
#  drop_col(col='formal_def_productiva', axis=1)
#  Index: 80 entries, 0 to 119
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     80 non-null     object 
#   1   pais          80 non-null     object 
#   2   cat_ocup_cod  70 non-null     float64
#   3   indicador     80 non-null     object 
#   4   valor         80 non-null     float64
#  
#  |    | geocodigo   | pais      |   cat_ocup_cod | indicador   |   valor |
#  |---:|:------------|:----------|---------------:|:------------|--------:|
#  |  0 | ARG         | Argentina |              1 | Empleadores |     3.7 |
#  
#  ------------------------------
#  
#  drop_col(col='cat_ocup_cod', axis=1)
#  Index: 80 entries, 0 to 119
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  80 non-null     object 
#   1   pais       80 non-null     object 
#   2   indicador  80 non-null     object 
#   3   valor      80 non-null     float64
#  
#  |    | geocodigo   | pais      | indicador   |   valor |
#  |---:|:------------|:----------|:------------|--------:|
#  |  0 | ARG         | Argentina | Empleadores |     3.7 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  Index: 80 entries, 0 to 119
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  80 non-null     object 
#   1   indicador  80 non-null     object 
#   2   valor      80 non-null     float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | ARG         | Empleadores |     3.7 |
#  
#  ------------------------------
#  