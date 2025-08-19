from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	multiplicar_por_escalar(col='participacion_gasto_publico_consolidado', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
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
#  |  0 |   1980 | Nacional            |                                   65.8758 |
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
#  |  0 |   1980 | Nacional            |                                   65.8758 |
#  
#  ------------------------------
#  