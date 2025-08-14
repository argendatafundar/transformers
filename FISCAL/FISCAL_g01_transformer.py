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
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   pais                    151 non-null    object 
#   1   codigo_pais             151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#  
#  |    | pais    | codigo_pais   |   gasto_publico_promedio |
#  |---:|:--------|:--------------|-------------------------:|
#  |  0 | Albania | ALB           |                  30.3236 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   pais                    151 non-null    object 
#   1   codigo_pais             151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#  
#  |    | pais    | codigo_pais   |   gasto_publico_promedio |
#  |---:|:--------|:--------------|-------------------------:|
#  |  0 | Albania | ALB           |                  30.3236 |
#  
#  ------------------------------
#  