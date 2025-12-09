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
#  RangeIndex: 212 entries, 0 to 211
#  Data columns (total 6 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   iso2                          211 non-null    object 
#   1   iso3                          208 non-null    object 
#   2   descripcion_pais              212 non-null    object 
#   3   gasto_receptivo               212 non-null    float64
#   4   categoria                     212 non-null    object 
#   5   distribucion_gasto_receptivo  212 non-null    float64
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_receptivo | categoria    |   distribucion_gasto_receptivo |
#  |---:|:-------|:-------|:-------------------|------------------:|:-------------|-------------------------------:|
#  |  0 | A19    |        | Resto de América   |              39.5 | No limítrofe |                       0.106254 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 212 entries, 0 to 211
#  Data columns (total 6 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   iso2                          211 non-null    object 
#   1   iso3                          208 non-null    object 
#   2   descripcion_pais              212 non-null    object 
#   3   gasto_receptivo               212 non-null    float64
#   4   categoria                     212 non-null    object 
#   5   distribucion_gasto_receptivo  212 non-null    float64
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_receptivo | categoria    |   distribucion_gasto_receptivo |
#  |---:|:-------|:-------|:-------------------|------------------:|:-------------|-------------------------------:|
#  |  0 | A19    |        | Resto de América   |              39.5 | No limítrofe |                       0.106254 |
#  
#  ------------------------------
#  