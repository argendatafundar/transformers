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
#  RangeIndex: 158 entries, 0 to 157
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         158 non-null    object 
#   1   geonombreFundar         158 non-null    object 
#   2   ultimo_anio_disponible  158 non-null    int64  
#   3   share_mujeres           158 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   share_mujeres |
#  |---:|:------------------|:------------------|-------------------------:|----------------:|
#  |  0 | MMR               | Myanmar           |                     2023 |         76.5406 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 158 entries, 0 to 157
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         158 non-null    object 
#   1   geonombreFundar         158 non-null    object 
#   2   ultimo_anio_disponible  158 non-null    int64  
#   3   share_mujeres           158 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   share_mujeres |
#  |---:|:------------------|:------------------|-------------------------:|----------------:|
#  |  0 | MMR               | Myanmar           |                     2023 |         76.5406 |
#  
#  ------------------------------
#  