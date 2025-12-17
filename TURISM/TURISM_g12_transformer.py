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
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 |
#  
#  ------------------------------
#  