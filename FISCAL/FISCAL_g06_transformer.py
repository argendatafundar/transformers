from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='gasto_publico_porcentaje_consolidado', k=100),
	replace_multiple_values(col='nivel_gobierno', replacements={'Nacional': 'Nación', 'Provincial': 'Provincia', 'Municipal': 'Municip.'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 4 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  3 non-null      int64  
#   1   nivel_gobierno                        3 non-null      object 
#   2   gasto_publico_porcentaje_pib          3 non-null      float64
#   3   gasto_publico_porcentaje_consolidado  3 non-null      float64
#  
#  |    |   anio | nivel_gobierno   |   gasto_publico_porcentaje_pib |   gasto_publico_porcentaje_consolidado |
#  |---:|-------:|:-----------------|-------------------------------:|---------------------------------------:|
#  |  0 |   2023 | Nacional         |                        22.2951 |                               0.532529 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='gasto_publico_porcentaje_consolidado', k=100)
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 4 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  3 non-null      int64  
#   1   nivel_gobierno                        3 non-null      object 
#   2   gasto_publico_porcentaje_pib          3 non-null      float64
#   3   gasto_publico_porcentaje_consolidado  3 non-null      float64
#  
#  |    |   anio | nivel_gobierno   |   gasto_publico_porcentaje_pib |   gasto_publico_porcentaje_consolidado |
#  |---:|-------:|:-----------------|-------------------------------:|---------------------------------------:|
#  |  0 |   2023 | Nación           |                        22.2951 |                                53.2529 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='nivel_gobierno', replacements={'Nacional': 'Nación', 'Provincial': 'Provincia', 'Municipal': 'Municip.'})
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 4 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  3 non-null      int64  
#   1   nivel_gobierno                        3 non-null      object 
#   2   gasto_publico_porcentaje_pib          3 non-null      float64
#   3   gasto_publico_porcentaje_consolidado  3 non-null      float64
#  
#  |    |   anio | nivel_gobierno   |   gasto_publico_porcentaje_pib |   gasto_publico_porcentaje_consolidado |
#  |---:|-------:|:-----------------|-------------------------------:|---------------------------------------:|
#  |  0 |   2023 | Nación           |                        22.2951 |                                53.2529 |
#  
#  ------------------------------
#  