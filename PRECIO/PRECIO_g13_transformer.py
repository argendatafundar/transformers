from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rescale(df:DataFrame, group_cols:list[str], summarised_col:str, new_col:str) -> DataFrame:
    df[new_col] = df.groupby(group_cols)[summarised_col].transform(
    lambda x: 100*(x/x.sum()))
    return df

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, None))
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='porcentaje', k=100),
	map_categoria(curr_col='rubro', new_col='rubro_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y prendas de vestir', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y prendas de vestir', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Transporte, comunicaciones y otros servicios', 'Transporte': 'Transporte, comunicaciones y otros servicios', 'Bienes y servicios varios': 'Transporte, comunicaciones y otros servicios'}),
	agg_sum(key_cols=['periodo', 'rubro_agrupado'], summarised_col='porcentaje'),
	rescale(group_cols=['periodo'], summarised_col='porcentaje', new_col='porcentaje_scaled'),
	drop_col(col='porcentaje', axis=1)
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
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   rubro           39 non-null     object 
#   1   periodo         39 non-null     object 
#   2   porcentaje      39 non-null     float64
#   3   rubro_agrupado  21 non-null     object 
#  
#  |    | rubro                              | periodo   |   porcentaje | rubro_agrupado   |
#  |---:|:-----------------------------------|:----------|-------------:|:-----------------|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |                  |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='rubro', new_col='rubro_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y prendas de vestir', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y prendas de vestir', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Transporte, comunicaciones y otros servicios', 'Transporte': 'Transporte, comunicaciones y otros servicios', 'Bienes y servicios varios': 'Transporte, comunicaciones y otros servicios'})
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   rubro           39 non-null     object 
#   1   periodo         39 non-null     object 
#   2   porcentaje      39 non-null     float64
#   3   rubro_agrupado  21 non-null     object 
#  
#  |    | rubro                              | periodo   |   porcentaje | rubro_agrupado   |
#  |---:|:-----------------------------------|:----------|-------------:|:-----------------|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |                  |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['periodo', 'rubro_agrupado'], summarised_col='porcentaje')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   periodo            15 non-null     object 
#   1   rubro_agrupado     15 non-null     object 
#   2   porcentaje         15 non-null     float64
#   3   porcentaje_scaled  15 non-null     float64
#  
#  |    | periodo   | rubro_agrupado          |   porcentaje |   porcentaje_scaled |
#  |---:|:----------|:------------------------|-------------:|--------------------:|
#  |  0 | 1996-1997 | Cultura y esparcimiento |          5.1 |             11.2088 |
#  
#  ------------------------------
#  
#  rescale(group_cols=['periodo'], summarised_col='porcentaje', new_col='porcentaje_scaled')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   periodo            15 non-null     object 
#   1   rubro_agrupado     15 non-null     object 
#   2   porcentaje         15 non-null     float64
#   3   porcentaje_scaled  15 non-null     float64
#  
#  |    | periodo   | rubro_agrupado          |   porcentaje |   porcentaje_scaled |
#  |---:|:----------|:------------------------|-------------:|--------------------:|
#  |  0 | 1996-1997 | Cultura y esparcimiento |          5.1 |             11.2088 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   periodo            15 non-null     object 
#   1   rubro_agrupado     15 non-null     object 
#   2   porcentaje_scaled  15 non-null     float64
#  
#  |    | periodo   | rubro_agrupado          |   porcentaje_scaled |
#  |---:|:----------|:------------------------|--------------------:|
#  |  0 | 1996-1997 | Cultura y esparcimiento |             11.2088 |
#  
#  ------------------------------
#  