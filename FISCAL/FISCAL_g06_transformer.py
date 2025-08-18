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
#  identity()
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