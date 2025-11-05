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
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    29 non-null     object 
#   1   geonombreFundar    29 non-null     object 
#   2   anio               29 non-null     int64  
#   3   tipo_informalidad  29 non-null     object 
#   4   valor              29 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | tipo_informalidad                    |   valor |
#  |---:|:------------------|:------------------|-------:|:-------------------------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Informalidad (definición productiva) |    40.1 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    29 non-null     object 
#   1   geonombreFundar    29 non-null     object 
#   2   anio               29 non-null     int64  
#   3   tipo_informalidad  29 non-null     object 
#   4   valor              29 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | tipo_informalidad                    |   valor |
#  |---:|:------------------|:------------------|-------:|:-------------------------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Informalidad (definición productiva) |    40.1 |
#  
#  ------------------------------
#  