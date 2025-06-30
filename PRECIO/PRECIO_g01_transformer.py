from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, None))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	map_categoria(curr_col='sector', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y prendas de vestir', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y prendas de vestir', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Transporte, comunicaciones y otros servicios', 'Transporte': 'Transporte, comunicaciones y otros servicios', 'Bienes y servicios varios': 'Transporte, comunicaciones y otros servicios'})
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
#  map_categoria(curr_col='sector', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y prendas de vestir', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y prendas de vestir', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Transporte, comunicaciones y otros servicios', 'Transporte': 'Transporte, comunicaciones y otros servicios', 'Bienes y servicios varios': 'Transporte, comunicaciones y otros servicios'})
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   sector           12 non-null     object 
#   1   valor            12 non-null     float64
#   2   sector_agrupado  12 non-null     object 
#  
#  |    | sector                             |   valor | sector_agrupado             |
#  |---:|:-----------------------------------|--------:|:----------------------------|
#  |  0 | Alimentos y bebidas no alcoholicas |   26.93 | Alimentos, bebidas y tabaco |
#  
#  ------------------------------
#  