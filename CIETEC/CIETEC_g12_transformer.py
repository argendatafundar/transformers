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
#  RangeIndex: 187 entries, 0 to 186
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype 
#  ---  ------                  --------------  ----- 
#   0   geocodigoFundar         187 non-null    object
#   1   geonombreFundar         187 non-null    object
#   2   anio                    187 non-null    int64 
#   3   patentes_de_residentes  187 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   patentes_de_residentes |
#  |---:|:------------------|:------------------|-------:|-------------------------:|
#  |  0 | ALB               | Albania           |   2023 |                       29 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 187 entries, 0 to 186
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype 
#  ---  ------                  --------------  ----- 
#   0   geocodigoFundar         187 non-null    object
#   1   geonombreFundar         187 non-null    object
#   2   anio                    187 non-null    int64 
#   3   patentes_de_residentes  187 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   patentes_de_residentes |
#  |---:|:------------------|:------------------|-------:|-------------------------:|
#  |  0 | ALB               | Albania           |   2023 |                       29 |
#  
#  ------------------------------
#  