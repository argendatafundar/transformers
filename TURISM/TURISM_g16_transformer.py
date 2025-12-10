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
#  RangeIndex: 276 entries, 0 to 275
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             276 non-null    int64  
#   1   tipo_vuelo       276 non-null    object 
#   2   fuente           276 non-null    object 
#   3   pasajeros_miles  226 non-null    float64
#  
#  |    |   anio | tipo_vuelo   | fuente                |   pasajeros_miles |
#  |---:|-------:|:-------------|:----------------------|------------------:|
#  |  0 |   1933 | Total        | Fundación Norte y Sur |           4.10589 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 276 entries, 0 to 275
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             276 non-null    int64  
#   1   tipo_vuelo       276 non-null    object 
#   2   fuente           276 non-null    object 
#   3   pasajeros_miles  226 non-null    float64
#  
#  |    |   anio | tipo_vuelo   | fuente                |   pasajeros_miles |
#  |---:|-------:|:-------------|:----------------------|------------------:|
#  |  0 |   1933 | Total        | Fundación Norte y Sur |           4.10589 |
#  
#  ------------------------------
#  