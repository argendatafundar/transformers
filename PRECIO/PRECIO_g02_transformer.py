from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, None))
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def rescale(df:DataFrame, group_cols:list[str], summarised_col:str) -> DataFrame:
    df['value_scaled'] = df.groupby(group_cols)[summarised_col].transform(
    lambda x: 100*(x/x.sum()))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	map_categoria(curr_col='sector', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y otros', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Comunicaciones', 'Transporte': 'Transporte', 'Bienes y servicios varios': 'Varios'}),
	agg_sum(key_cols=['region', 'sector_agrupado'], summarised_col='valor'),
	rescale(group_cols=['region'], summarised_col='valor'),
	drop_col(col='valor', axis=1),
	ordenar_dos_columnas(col1='region', order1=['NEA', 'NOA', 'Pampeana', 'Cuyo', 'Patagonia', 'Nacional', 'GBA'], col2='sector_agrupado', order2=['Alimentos, bebidas y tabaco', 'Cultura y esparcimiento', 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado', 'Transporte', 'Educación y salud', 'Varios', 'Comunicaciones'])
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
#  map_categoria(curr_col='sector', new_col='sector_agrupado', mapper={'Alimentos y bebidas no alcoholicas': 'Alimentos, bebidas y tabaco', 'Bebidas alcoholicas y tabaco': 'Alimentos, bebidas y tabaco', 'Educacion': 'Educación y salud', 'Salud': 'Educación y salud', 'Recreacion y cultura': 'Cultura y esparcimiento', 'Restaurantes y hoteles': 'Cultura y esparcimiento', 'Vivienda agua electricidad gas y otros combustibles': 'Vivienda, equipamiento y otros', 'Equipamiento y mantenimiento del hogar': 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado': 'Prendas de vestir y calzado', 'Comunicaciones': 'Comunicaciones', 'Transporte': 'Transporte', 'Bienes y servicios varios': 'Varios'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region           84 non-null     object 
#   1   sector           84 non-null     object 
#   2   valor            84 non-null     float64
#   3   sector_agrupado  84 non-null     object 
#  
#  |    | region   | sector                             |   valor | sector_agrupado             |
#  |---:|:---------|:-----------------------------------|--------:|:----------------------------|
#  |  0 | GBA      | Alimentos y bebidas no alcoholicas |    23.4 | Alimentos, bebidas y tabaco |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['region', 'sector_agrupado'], summarised_col='valor')
#  RangeIndex: 56 entries, 0 to 55
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region           56 non-null     object 
#   1   sector_agrupado  56 non-null     object 
#   2   valor            56 non-null     float64
#   3   value_scaled     56 non-null     float64
#  
#  |    | region   | sector_agrupado             |   valor |   value_scaled |
#  |---:|:---------|:----------------------------|--------:|---------------:|
#  |  0 | Cuyo     | Alimentos, bebidas y tabaco |      32 |         32.032 |
#  
#  ------------------------------
#  
#  rescale(group_cols=['region'], summarised_col='valor')
#  RangeIndex: 56 entries, 0 to 55
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region           56 non-null     object 
#   1   sector_agrupado  56 non-null     object 
#   2   valor            56 non-null     float64
#   3   value_scaled     56 non-null     float64
#  
#  |    | region   | sector_agrupado             |   valor |   value_scaled |
#  |---:|:---------|:----------------------------|--------:|---------------:|
#  |  0 | Cuyo     | Alimentos, bebidas y tabaco |      32 |         32.032 |
#  
#  ------------------------------
#  
#  drop_col(col='valor', axis=1)
#  RangeIndex: 56 entries, 0 to 55
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   region           56 non-null     category
#   1   sector_agrupado  56 non-null     category
#   2   value_scaled     56 non-null     float64 
#  
#  |    | region   | sector_agrupado             |   value_scaled |
#  |---:|:---------|:----------------------------|---------------:|
#  |  0 | Cuyo     | Alimentos, bebidas y tabaco |         32.032 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='region', order1=['NEA', 'NOA', 'Pampeana', 'Cuyo', 'Patagonia', 'Nacional', 'GBA'], col2='sector_agrupado', order2=['Alimentos, bebidas y tabaco', 'Cultura y esparcimiento', 'Vivienda, equipamiento y otros', 'Prendas de vestir y calzado', 'Transporte', 'Educación y salud', 'Varios', 'Comunicaciones'])
#  Index: 56 entries, 16 to 9
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   region           56 non-null     category
#   1   sector_agrupado  56 non-null     category
#   2   value_scaled     56 non-null     float64 
#  
#  |    | region   | sector_agrupado             |   value_scaled |
#  |---:|:---------|:----------------------------|---------------:|
#  | 16 | NEA      | Alimentos, bebidas y tabaco |           38.9 |
#  
#  ------------------------------
#  