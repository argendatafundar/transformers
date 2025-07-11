from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 989 entries, 0 to 988
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  989 non-null    object
#   1   geonombreFundar  989 non-null    object
#   2   anio             989 non-null    int64 
#   3   valor            989 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ARG               | Argentina         |   1990 |    1634 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 989 entries, 0 to 988
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  989 non-null    object
#   1   geonombreFundar  989 non-null    object
#   2   anio             989 non-null    int64 
#   3   valor            989 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ARG               | Argentina         |   1990 |    1634 |
#  
#  ------------------------------
#  