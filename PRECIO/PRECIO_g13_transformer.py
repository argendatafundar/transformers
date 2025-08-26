from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, None))
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rescale(df:DataFrame, group_cols:list[str], summarised_col:str) -> DataFrame:
    df['value_scaled'] = df.groupby(group_cols)[summarised_col].transform(
    lambda x: 100*(x/x.sum()))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='porcentaje', k=100),
	query(condition="rubro != 'Total gasto de consumo'"),
	map_categoria(curr_col='rubro', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y otros', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Comunicaciones', 'Transporte': 'Transporte', 'Bienes y servicios varios': 'Varios'}),
	agg_sum(key_cols=['periodo', 'sector_agrupado'], summarised_col='porcentaje'),
	rescale(group_cols=['periodo'], summarised_col='porcentaje'),
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
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   rubro            36 non-null     object 
#   1   periodo          36 non-null     object 
#   2   porcentaje       36 non-null     float64
#   3   sector_agrupado  21 non-null     object 
#  
#  |    | rubro                              | periodo   |   porcentaje | sector_agrupado   |
#  |---:|:-----------------------------------|:----------|-------------:|:------------------|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |                   |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='rubro', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y otros', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Comunicaciones', 'Transporte': 'Transporte', 'Bienes y servicios varios': 'Varios'})
#  Index: 36 entries, 0 to 38
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   rubro            36 non-null     object 
#   1   periodo          36 non-null     object 
#   2   porcentaje       36 non-null     float64
#   3   sector_agrupado  21 non-null     object 
#  
#  |    | rubro                              | periodo   |   porcentaje | sector_agrupado   |
#  |---:|:-----------------------------------|:----------|-------------:|:------------------|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |                   |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['periodo', 'sector_agrupado'], summarised_col='porcentaje')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   periodo          21 non-null     object 
#   1   sector_agrupado  21 non-null     object 
#   2   porcentaje       21 non-null     float64
#   3   value_scaled     21 non-null     float64
#  
#  |    | periodo   | sector_agrupado   |   porcentaje |   value_scaled |
#  |---:|:----------|:------------------|-------------:|---------------:|
#  |  0 | 1996-1997 | Comunicaciones    |          2.6 |        5.71429 |
#  
#  ------------------------------
#  
#  rescale(group_cols=['periodo'], summarised_col='porcentaje')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   periodo          21 non-null     object 
#   1   sector_agrupado  21 non-null     object 
#   2   porcentaje       21 non-null     float64
#   3   value_scaled     21 non-null     float64
#  
#  |    | periodo   | sector_agrupado   |   porcentaje |   value_scaled |
#  |---:|:----------|:------------------|-------------:|---------------:|
#  |  0 | 1996-1997 | Comunicaciones    |          2.6 |        5.71429 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   periodo          21 non-null     object 
#   1   sector_agrupado  21 non-null     object 
#   2   value_scaled     21 non-null     float64
#  
#  |    | periodo   | sector_agrupado   |   value_scaled |
#  |---:|:----------|:------------------|---------------:|
#  |  0 | 1996-1997 | Comunicaciones    |        5.71429 |
#  
#  ------------------------------
#  