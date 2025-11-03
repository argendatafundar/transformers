from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='tasa_actividad', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 648 entries, 0 to 647
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            648 non-null    int64  
#   1   edad            648 non-null    int64  
#   2   tasa_actividad  648 non-null    float64
#  
#  |    |   anio |   edad |   tasa_actividad |
#  |---:|-------:|-------:|-----------------:|
#  |  0 |   2016 |     10 |       0.00156134 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_actividad', k=100)
#  RangeIndex: 648 entries, 0 to 647
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            648 non-null    int64  
#   1   edad            648 non-null    int64  
#   2   tasa_actividad  648 non-null    float64
#  
#  |    |   anio |   edad |   tasa_actividad |
#  |---:|-------:|-------:|-----------------:|
#  |  0 |   2016 |     10 |         0.156134 |
#  
#  ------------------------------
#  