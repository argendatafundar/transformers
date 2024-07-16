from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_auto': 'categoria', 'mineral_critico': 'indicador', 'mineral_utilizado_kg_por_vehiculo': 'valor'}),
	str_to_title(col='indicador'),
	replace_values(col='indicador', values={'Lithium': 'Litio', 'Niquel': 'Níquel', 'Graphite': 'Grafito', 'Cinc': 'Zinc', 'Tierras_Raras': 'Tierras raras'}),
	replace_values(col='categoria', values={'auto_electrico': 'Auto eléctrico', 'auto_convencional': 'Auto convencional'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column                             Non-Null Count  Dtype  
#  ---  ------                             --------------  -----  
#   0   tipo_auto                          18 non-null     object 
#   1   mineral_critico                    18 non-null     object 
#   2   mineral_utilizado_kg_por_vehiculo  18 non-null     float64
#  
#  |    | tipo_auto      | mineral_critico   |   mineral_utilizado_kg_por_vehiculo |
#  |---:|:---------------|:------------------|------------------------------------:|
#  |  0 | auto_electrico | cobre             |                                53.2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_auto': 'categoria', 'mineral_critico': 'indicador', 'mineral_utilizado_kg_por_vehiculo': 'valor'})
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  18 non-null     object 
#   1   indicador  18 non-null     object 
#   2   valor      18 non-null     float64
#  
#  |    | categoria      | indicador   |   valor |
#  |---:|:---------------|:------------|--------:|
#  |  0 | auto_electrico | Cobre       |    53.2 |
#  
#  ------------------------------
#  
#  str_to_title(col='indicador')
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  18 non-null     object 
#   1   indicador  18 non-null     object 
#   2   valor      18 non-null     float64
#  
#  |    | categoria      | indicador   |   valor |
#  |---:|:---------------|:------------|--------:|
#  |  0 | auto_electrico | Cobre       |    53.2 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'Lithium': 'Litio', 'Niquel': 'Níquel', 'Graphite': 'Grafito', 'Cinc': 'Zinc', 'Tierras_Raras': 'Tierras raras'})
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  18 non-null     object 
#   1   indicador  18 non-null     object 
#   2   valor      18 non-null     float64
#  
#  |    | categoria      | indicador   |   valor |
#  |---:|:---------------|:------------|--------:|
#  |  0 | auto_electrico | Cobre       |    53.2 |
#  
#  ------------------------------
#  
#  replace_values(col='categoria', values={'auto_electrico': 'Auto eléctrico', 'auto_convencional': 'Auto convencional'})
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  18 non-null     object 
#   1   indicador  18 non-null     object 
#   2   valor      18 non-null     float64
#  
#  |    | categoria      | indicador   |   valor |
#  |---:|:---------------|:------------|--------:|
#  |  0 | Auto eléctrico | Cobre       |    53.2 |
#  
#  ------------------------------
#  