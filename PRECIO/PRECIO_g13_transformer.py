from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='porcentaje', k=100),
	query(condition="rubro != 'Total gasto de consumo'"),
	replace_multiple_values(col='rubro', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda, agua, electricidad, gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcohólicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcohólicas': 'Alimentos y bebidas'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       39 non-null     object 
#   1   periodo     39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |        0.288 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='porcentaje', k=100)
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       39 non-null     object 
#   1   periodo     39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |
#  
#  ------------------------------
#  
#  query(condition="rubro != 'Total gasto de consumo'")
#  Index: 36 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       36 non-null     object 
#   1   periodo     36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | rubro               | periodo   |   porcentaje |
#  |---:|:--------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas | 1996-1997 |         28.8 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='rubro', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda, agua, electricidad, gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcohólicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcohólicas': 'Alimentos y bebidas'})
#  Index: 36 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       36 non-null     object 
#   1   periodo     36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | rubro               | periodo   |   porcentaje |
#  |---:|:--------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas | 1996-1997 |         28.8 |
#  
#  ------------------------------
#  