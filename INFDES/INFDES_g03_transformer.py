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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'iso3': 'geocodigo', 'cat_ocup_detalle': 'indicador'})
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
#   3   cat_ocup_detalle       120 non-null    object 
#   4   formal_def_productiva  105 non-null    object 
#   5   valor                  120 non-null    float64
#   6   cat_ocup_cod           105 non-null    float64
#  
#  |    | iso3   | pais      |   anio | cat_ocup_detalle                           | formal_def_productiva   |   valor |   cat_ocup_cod |
#  |---:|:-------|:----------|-------:|:-------------------------------------------|:------------------------|--------:|---------------:|
#  |  0 | ARG    | Argentina |   2022 | Asalariados en pequeñas y grandes empresas | Formal                  |    32.4 |              2 |
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
#   2   cat_ocup_detalle       80 non-null     object 
#   3   formal_def_productiva  70 non-null     object 
#   4   valor                  80 non-null     float64
#   5   cat_ocup_cod           70 non-null     float64
#  
#  |    | iso3   | pais      | cat_ocup_detalle                           | formal_def_productiva   |   valor |   cat_ocup_cod |
#  |---:|:-------|:----------|:-------------------------------------------|:------------------------|--------:|---------------:|
#  |  0 | ARG    | Argentina | Asalariados en pequeñas y grandes empresas | Formal                  |    32.4 |              2 |
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
#   2   indicador              80 non-null     object 
#   3   formal_def_productiva  70 non-null     object 
#   4   valor                  80 non-null     float64
#   5   cat_ocup_cod           70 non-null     float64
#  
#  |    | geocodigo   | pais      | indicador                                  | formal_def_productiva   |   valor |   cat_ocup_cod |
#  |---:|:------------|:----------|:-------------------------------------------|:------------------------|--------:|---------------:|
#  |  0 | ARG         | Argentina | Asalariados en pequeñas y grandes empresas | Formal                  |    32.4 |              2 |
#  
#  ------------------------------
#  