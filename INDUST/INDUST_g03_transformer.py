from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['ciiu_rev3_2d', 'rama_de_actividad'], axis=1),
	replace_value(col='descripcion_corta', curr_value=None, new_value=None, mapping={'Productos minerales no metálicos': 'Minerales no metálicos', 'Maquinaria de oficina e informática': 'Equipos de oficina', 'Instrumentos médicos y de precisión': 'Instrumental médico'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 720 entries, 0 to 719
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    720 non-null    int64  
#   1   ciiu_rev3_2d            720 non-null    int64  
#   2   rama_de_actividad       720 non-null    object 
#   3   descripcion_corta       720 non-null    object 
#   4   salario_respecto_media  720 non-null    float64
#  
#  |    |   anio |   ciiu_rev3_2d | rama_de_actividad   | descripcion_corta   |   salario_respecto_media |
#  |---:|-------:|---------------:|:--------------------|:--------------------|-------------------------:|
#  |  0 |   1995 |             15 | Alimentos           | Alimentos y bebidas |                  3.64005 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 29 to 719
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    24 non-null     int64  
#   1   ciiu_rev3_2d            24 non-null     int64  
#   2   rama_de_actividad       24 non-null     object 
#   3   descripcion_corta       24 non-null     object 
#   4   salario_respecto_media  24 non-null     float64
#  
#  |    |   anio |   ciiu_rev3_2d | rama_de_actividad   | descripcion_corta   |   salario_respecto_media |
#  |---:|-------:|---------------:|:--------------------|:--------------------|-------------------------:|
#  | 29 |   2024 |             15 | Alimentos           | Alimentos y bebidas |                  12.7491 |
#  
#  ------------------------------
#  
#  drop_col(col=['ciiu_rev3_2d', 'rama_de_actividad'], axis=1)
#  Index: 24 entries, 29 to 719
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    24 non-null     int64  
#   1   descripcion_corta       24 non-null     object 
#   2   salario_respecto_media  24 non-null     float64
#  
#  |    |   anio | descripcion_corta   |   salario_respecto_media |
#  |---:|-------:|:--------------------|-------------------------:|
#  | 29 |   2024 | Alimentos y bebidas |                  12.7491 |
#  
#  ------------------------------
#  
#  replace_value(col='descripcion_corta', curr_value=None, new_value=None, mapping={'Productos minerales no metálicos': 'Minerales no metálicos', 'Maquinaria de oficina e informática': 'Equipos de oficina', 'Instrumentos médicos y de precisión': 'Instrumental médico'})
#  Index: 24 entries, 29 to 719
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    24 non-null     int64  
#   1   descripcion_corta       24 non-null     object 
#   2   salario_respecto_media  24 non-null     float64
#  
#  |    |   anio | descripcion_corta   |   salario_respecto_media |
#  |---:|-------:|:--------------------|-------------------------:|
#  | 29 |   2024 | Alimentos y bebidas |                  12.7491 |
#  
#  ------------------------------
#  