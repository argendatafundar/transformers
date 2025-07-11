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
#  RangeIndex: 4699 entries, 0 to 4698
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype 
#  ---  ------                  --------------  ----- 
#   0   geocodigoFundar         4699 non-null   object
#   1   geonombreFundar         4699 non-null   object
#   2   anio                    4699 non-null   int64 
#   3   patentes_de_residentes  4699 non-null   int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   patentes_de_residentes |
#  |---:|:------------------|:------------------|-------:|-------------------------:|
#  |  0 | ALB               | Albania           |   1997 |                        1 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 4699 entries, 0 to 4698
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype 
#  ---  ------                  --------------  ----- 
#   0   geocodigoFundar         4699 non-null   object
#   1   geonombreFundar         4699 non-null   object
#   2   anio                    4699 non-null   int64 
#   3   patentes_de_residentes  4699 non-null   int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   patentes_de_residentes |
#  |---:|:------------------|:------------------|-------:|-------------------------:|
#  |  0 | ALB               | Albania           |   1997 |                        1 |
#  
#  ------------------------------
#  