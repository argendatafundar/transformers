from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='sector', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcoholicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcoholicas': 'Alimentos y bebidas'})
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
#  replace_multiple_values(col='sector', replacements={'Prendas de vestir y calzado': 'Vestimenta', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, serv. púb. y comb.', 'Equipamiento y mantenimiento del hogar': 'Equip. y mant. hogar', 'Bienes y servicios varios': 'Varios', 'Bebidas alcoholicas y tabaco': 'Alcohol y tabaco', 'Alimentos y bebidas no alcoholicas': 'Alimentos y bebidas'})
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  12 non-null     object 
#   1   valor   12 non-null     float64
#  
#  |    | sector              |   valor |
#  |---:|:--------------------|--------:|
#  |  0 | Alimentos y bebidas |   26.93 |
#  
#  ------------------------------
#  