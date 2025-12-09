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
#  RangeIndex: 1243 entries, 0 to 1242
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               1243 non-null   int64  
#   1   geocodigoFundar    1243 non-null   object 
#   2   geonombreFundar    1243 non-null   object 
#   3   share_tourism_gdp  1243 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_tourism_gdp |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   2008 | ALB               | Albania           |             2.75707 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 1243 entries, 0 to 1242
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               1243 non-null   int64  
#   1   geocodigoFundar    1243 non-null   object 
#   2   geonombreFundar    1243 non-null   object 
#   3   share_tourism_gdp  1243 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_tourism_gdp |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   2008 | ALB               | Albania           |             2.75707 |
#  
#  ------------------------------
#  