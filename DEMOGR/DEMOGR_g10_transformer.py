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
#  RangeIndex: 17538 entries, 0 to 17537
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             17538 non-null  int64  
#   1   geocodigoFundar  17538 non-null  object 
#   2   geonombreFundar  17538 non-null  object 
#   3   tgf_adolescente  17538 non-null  float64
#   4   fuente           17538 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   tgf_adolescente | fuente                          |
#  |---:|-------:|:------------------|:------------------|------------------:|:--------------------------------|
#  |  0 |   1950 | ABW               | Aruba             |           27.9817 | World Population Prospects (UN) |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 17538 entries, 0 to 17537
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             17538 non-null  int64  
#   1   geocodigoFundar  17538 non-null  object 
#   2   geonombreFundar  17538 non-null  object 
#   3   tgf_adolescente  17538 non-null  float64
#   4   fuente           17538 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   tgf_adolescente | fuente                          |
#  |---:|-------:|:------------------|:------------------|------------------:|:--------------------------------|
#  |  0 |   1950 | ABW               | Aruba             |           27.9817 | World Population Prospects (UN) |
#  
#  ------------------------------
#  