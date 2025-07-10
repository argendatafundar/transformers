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
#  RangeIndex: 147 entries, 0 to 146
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  147 non-null    object 
#   1   geonombreFundar  147 non-null    object 
#   2   anio             147 non-null    int64  
#   3   valor            147 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | KOR               | Corea del Sur     |   2023 |  7308.8 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 147 entries, 0 to 146
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  147 non-null    object 
#   1   geonombreFundar  147 non-null    object 
#   2   anio             147 non-null    int64  
#   3   valor            147 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | KOR               | Corea del Sur     |   2023 |  7308.8 |
#  
#  ------------------------------
#  