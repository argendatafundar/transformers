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
	multiplicar_por_escalar(col='share', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   4 non-null      int64  
#   1   especie                4 non-null      object 
#   2   desembarque_toneladas  4 non-null      float64
#   3   share                  4 non-null      float64
#  
#  |    |   anio | especie       |   desembarque_toneladas |    share |
#  |---:|-------:|:--------------|------------------------:|---------:|
#  |  0 |   2024 | Calamar Illex |                  154567 | 0.188151 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share', k=100)
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   4 non-null      int64  
#   1   especie                4 non-null      object 
#   2   desembarque_toneladas  4 non-null      float64
#   3   share                  4 non-null      float64
#  
#  |    |   anio | especie       |   desembarque_toneladas |   share |
#  |---:|-------:|:--------------|------------------------:|--------:|
#  |  0 |   2024 | Calamar Illex |                  154567 | 18.8151 |
#  
#  ------------------------------
#  