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
	multiplicar_por_escalar(col='desembarque_toneladas', k=0.001)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 144 entries, 0 to 143
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   144 non-null    int64  
#   1   especie_agregada       144 non-null    object 
#   2   desembarque_toneladas  144 non-null    float64
#  
#  |    |   anio | especie_agregada   |   desembarque_toneladas |
#  |---:|-------:|:-------------------|------------------------:|
#  |  0 |   1989 | Calamar Illex      |                 23101.7 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='desembarque_toneladas', k=0.001)
#  RangeIndex: 144 entries, 0 to 143
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   144 non-null    int64  
#   1   especie_agregada       144 non-null    object 
#   2   desembarque_toneladas  144 non-null    float64
#  
#  |    |   anio | especie_agregada   |   desembarque_toneladas |
#  |---:|-------:|:-------------------|------------------------:|
#  |  0 |   1989 | Calamar Illex      |                 23.1017 |
#  
#  ------------------------------
#  