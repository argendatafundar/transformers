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
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  17 non-null     object 
#   1   geonombreFundar  17 non-null     object 
#   2   salariohorario   17 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   salariohorario |
#  |---:|:------------------|:------------------|-----------------:|
#  |  0 | HND               | Honduras          |              2.7 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  17 non-null     object 
#   1   geonombreFundar  17 non-null     object 
#   2   salariohorario   17 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   salariohorario |
#  |---:|:------------------|:------------------|-----------------:|
#  |  0 | HND               | Honduras          |              2.7 |
#  
#  ------------------------------
#  