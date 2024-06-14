from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(iso3='geoselector')
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
#  rename_columns(iso3='geoselector')
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