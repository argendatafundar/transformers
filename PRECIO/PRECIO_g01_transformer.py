from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(sector='categoria'),
	replace_value(col='categoria', curr_value='Alimentos y bebidas no alcoholicas', new_value='Alimentos y\nbebidas no\nalcoholicas'),
	replace_value(col='categoria', curr_value='Bebidas alcoholicas y tabaco', new_value='Bebidas\nalcoholicas y\ntabaco'),
	replace_value(col='categoria', curr_value='Bienes y servicios varios', new_value='Bienes y\nservicios\nvarios'),
	replace_value(col='categoria', curr_value='Comunicaciones', new_value='Comunicaciones'),
	replace_value(col='categoria', curr_value='Educacion', new_value='Educacion'),
	replace_value(col='categoria', curr_value='Equipamiento y mantenimiento del hogar', new_value='Equipamiento y\nmantenimiento\ndel hogar'),
	replace_value(col='categoria', curr_value='Prendas de vestir y calzado', new_value='Prendas de\nvestir y\ncalzado'),
	replace_value(col='categoria', curr_value='Recreacion y cultura', new_value='Recreacion y\ncultura'),
	replace_value(col='categoria', curr_value='Restaurantes y hoteles', new_value='Restaurantes y\nhoteles'),
	replace_value(col='categoria', curr_value='Salud', new_value='Salud'),
	replace_value(col='categoria', curr_value='Transporte', new_value='Transporte'),
	replace_value(col='categoria', curr_value='Vivienda agua electricidad gas y otros combustibles', new_value='Vivienda agua\nelectricidad\ngas y otros\ncombustibles')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  12 non-null     object 
#   1   valor   12 non-null     float64
#  
#  |    | sector                             |   valor |
#  |---:|:-----------------------------------|--------:|
#  |  0 | Alimentos y bebidas no alcoholicas |   26.93 |
#  
#  ------------------------------
#  
#  rename_columns(sector='categoria')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria                          |   valor |
#  |---:|:-----------------------------------|--------:|
#  |  0 | Alimentos y bebidas no alcoholicas |   26.93 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Alimentos y bebidas no alcoholicas', new_value='Alimentos y\nbebidas no\nalcoholicas')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Bebidas alcoholicas y tabaco', new_value='Bebidas\nalcoholicas y\ntabaco')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Bienes y servicios varios', new_value='Bienes y\nservicios\nvarios')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Comunicaciones', new_value='Comunicaciones')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Educacion', new_value='Educacion')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Equipamiento y mantenimiento del hogar', new_value='Equipamiento y\nmantenimiento\ndel hogar')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Prendas de vestir y calzado', new_value='Prendas de\nvestir y\ncalzado')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Recreacion y cultura', new_value='Recreacion y\ncultura')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Restaurantes y hoteles', new_value='Restaurantes y\nhoteles')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Salud', new_value='Salud')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Transporte', new_value='Transporte')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Vivienda agua electricidad gas y otros combustibles', new_value='Vivienda agua\nelectricidad\ngas y otros\ncombustibles')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  12 non-null     object 
#   1   valor      12 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Alimentos y |   26.93 |
#  |    | bebidas no  |         |
#  |    | alcoholicas |         |
#  
#  ------------------------------
#  