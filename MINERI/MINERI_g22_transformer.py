from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_auto': 'serie', 'mineral_critico': 'indicador', 'mineral_utilizado_kg_por_vehiculo': 'valor'})
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
#  rename_cols(map={'tipo_auto': 'serie', 'mineral_critico': 'indicador', 'mineral_utilizado_kg_por_vehiculo': 'valor'})
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   serie      18 non-null     object 
#   1   indicador  18 non-null     object 
#   2   valor      18 non-null     float64
#  
#  |    | serie          | indicador   |   valor |
#  |---:|:---------------|:------------|--------:|
#  |  0 | auto_electrico | cobre       |    53.2 |
#  
#  ------------------------------
#  