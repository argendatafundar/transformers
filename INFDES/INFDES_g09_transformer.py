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
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  22 non-null     object 
#   1   geonombreFundar  22 non-null     object 
#   2   anio_circa       22 non-null     int64  
#   3   brecha           22 non-null     float64
#   4   circa            22 non-null     int64  
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio_circa |   brecha |   circa |
#  |---:|:------------------|:------------------|-------------:|---------:|--------:|
#  |  0 | ARG               | Argentina         |         2000 |  13.9852 |    2000 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  22 non-null     object 
#   1   geonombreFundar  22 non-null     object 
#   2   anio_circa       22 non-null     int64  
#   3   brecha           22 non-null     float64
#   4   circa            22 non-null     int64  
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio_circa |   brecha |   circa |
#  |---:|:------------------|:------------------|-------------:|---------:|--------:|
#  |  0 | ARG               | Argentina         |         2000 |  13.9852 |    2000 |
#  
#  ------------------------------
#  