from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17047 entries, 0 to 17046
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  17047 non-null  object 
#   1   geonombreFundar  17047 non-null  object 
#   2   anio             17047 non-null  int64  
#   3   relativo_arg     15936 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   relativo_arg |
#  |---:|:------------------|:------------------|-------:|---------------:|
#  |  0 | AFG               | Afganistán        |   1950 |         6.8763 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  RangeIndex: 17047 entries, 0 to 17046
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  17047 non-null  object 
#   1   anio             17047 non-null  int64  
#   2   relativo_arg     15936 non-null  float64
#  
#  |    | geonombreFundar   |   anio |   relativo_arg |
#  |---:|:------------------|-------:|---------------:|
#  |  0 | Afganistán        |   1950 |         6.8763 |
#  
#  ------------------------------
#  