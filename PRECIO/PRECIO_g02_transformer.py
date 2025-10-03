from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='sector', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcoholicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcoholicas': 'Alimentos y bebidas'}),
	ordenar_dos_columnas(col1='region', order1=['NEA', 'NOA', 'Pampeana', 'Cuyo', 'Patagonia', 'Nacional', 'GBA'], col2='sector', order2=['Alimentos y bebidas', 'Vestimenta', 'Transporte', 'Vivienda, serv. púb. y comb.', 'Equip. y mant. hogar', 'Recreacion y cultura', 'Salud', 'Restaurantes y hoteles', 'Alcohol y tabaco', 'Varios', 'Comunicaciones', 'Educacion'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   region  84 non-null     object 
#   1   sector  84 non-null     object 
#   2   valor   84 non-null     float64
#  
#  |    | region   | sector                             |   valor |
#  |---:|:---------|:-----------------------------------|--------:|
#  |  0 | GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='sector', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcoholicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcoholicas': 'Alimentos y bebidas'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype   
#  ---  ------  --------------  -----   
#   0   region  84 non-null     category
#   1   sector  84 non-null     category
#   2   valor   84 non-null     float64 
#  
#  |    | region   | sector              |   valor |
#  |---:|:---------|:--------------------|--------:|
#  |  0 | GBA      | Alimentos y bebidas |    23.4 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='region', order1=['NEA', 'NOA', 'Pampeana', 'Cuyo', 'Patagonia', 'Nacional', 'GBA'], col2='sector', order2=['Alimentos y bebidas', 'Vestimenta', 'Transporte', 'Vivienda, serv. púb. y comb.', 'Equip. y mant. hogar', 'Recreacion y cultura', 'Salud', 'Restaurantes y hoteles', 'Alcohol y tabaco', 'Varios', 'Comunicaciones', 'Educacion'])
#  Index: 84 entries, 2 to 54
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype   
#  ---  ------  --------------  -----   
#   0   region  84 non-null     category
#   1   sector  84 non-null     category
#   2   valor   84 non-null     float64 
#  
#  |    | region   | sector              |   valor |
#  |---:|:---------|:--------------------|--------:|
#  |  2 | NEA      | Alimentos y bebidas |    35.3 |
#  
#  ------------------------------
#  