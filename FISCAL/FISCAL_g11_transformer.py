from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 44 entries, 0 to 43
#  Data columns (total 2 columns):
#   #   Column                                       Non-Null Count  Dtype  
#  ---  ------                                       --------------  -----  
#   0   anio                                         44 non-null     int64  
#   1   indice_gasto_publico_consolidado_per_capita  44 non-null     float64
#  
#  |    |   anio |   indice_gasto_publico_consolidado_per_capita |
#  |---:|-------:|----------------------------------------------:|
#  |  0 |   1980 |                                             1 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 44 entries, 0 to 43
#  Data columns (total 2 columns):
#   #   Column                                       Non-Null Count  Dtype  
#  ---  ------                                       --------------  -----  
#   0   anio                                         44 non-null     int64  
#   1   indice_gasto_publico_consolidado_per_capita  44 non-null     float64
#  
#  |    |   anio |   indice_gasto_publico_consolidado_per_capita |
#  |---:|-------:|----------------------------------------------:|
#  |  0 |   1980 |                                             1 |
#  
#  ------------------------------
#  