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
	multiplicar_por_escalar(col='participacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2 entries, 0 to 1
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           2 non-null      int64  
#   1   sector         2 non-null      object 
#   2   indicador      2 non-null      object 
#   3   participacion  2 non-null      float64
#  
#  |    |   anio | sector   | indicador   |   participacion |
#  |---:|-------:|:---------|:------------|----------------:|
#  |  0 |   2024 | Pesca    | PIB         |       0.0030296 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion', k=100)
#  RangeIndex: 2 entries, 0 to 1
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           2 non-null      int64  
#   1   sector         2 non-null      object 
#   2   indicador      2 non-null      object 
#   3   participacion  2 non-null      float64
#  
#  |    |   anio | sector   | indicador   |   participacion |
#  |---:|-------:|:---------|:------------|----------------:|
#  |  0 |   2024 | Pesca    | PIB         |         0.30296 |
#  
#  ------------------------------
#  