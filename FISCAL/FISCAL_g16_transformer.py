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
	multiplicar_por_escalar(col='participacion_gasto_publico_consolidado', k=100),
	replace_multiple_values(col='nivel_de_gobierno', replacements={'Municipal': 'Municip.', 'Provincial': 'Provincia', 'Nacional': 'Nación'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 3 columns):
#   #   Column                                   Non-Null Count  Dtype  
#  ---  ------                                   --------------  -----  
#   0   anio                                     132 non-null    int64  
#   1   nivel_de_gobierno                        132 non-null    object 
#   2   participacion_gasto_publico_consolidado  132 non-null    float64
#  
#  |    |   anio | nivel_de_gobierno   |   participacion_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|------------------------------------------:|
#  |  0 |   1980 | Nacional            |                                  0.658758 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion_gasto_publico_consolidado', k=100)
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 3 columns):
#   #   Column                                   Non-Null Count  Dtype  
#  ---  ------                                   --------------  -----  
#   0   anio                                     132 non-null    int64  
#   1   nivel_de_gobierno                        132 non-null    object 
#   2   participacion_gasto_publico_consolidado  132 non-null    float64
#  
#  |    |   anio | nivel_de_gobierno   |   participacion_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|------------------------------------------:|
#  |  0 |   1980 | Nación              |                                   65.8758 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='nivel_de_gobierno', replacements={'Municipal': 'Municip.', 'Provincial': 'Provincia', 'Nacional': 'Nación'})
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 3 columns):
#   #   Column                                   Non-Null Count  Dtype  
#  ---  ------                                   --------------  -----  
#   0   anio                                     132 non-null    int64  
#   1   nivel_de_gobierno                        132 non-null    object 
#   2   participacion_gasto_publico_consolidado  132 non-null    float64
#  
#  |    |   anio | nivel_de_gobierno   |   participacion_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|------------------------------------------:|
#  |  0 |   1980 | Nación              |                                   65.8758 |
#  
#  ------------------------------
#  