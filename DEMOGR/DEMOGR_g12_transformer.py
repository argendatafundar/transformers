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
#  RangeIndex: 18012 entries, 0 to 18011
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               18012 non-null  int64  
#   1   geocodigoFundar    18012 non-null  object 
#   2   geonombreFundar    18012 non-null  object 
#   3   exp_vida_al_nacer  18012 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   exp_vida_al_nacer |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   1950 | BDI               | Burundi           |             40.9382 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 18012 entries, 0 to 18011
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               18012 non-null  int64  
#   1   geocodigoFundar    18012 non-null  object 
#   2   geonombreFundar    18012 non-null  object 
#   3   exp_vida_al_nacer  18012 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   exp_vida_al_nacer |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   1950 | BDI               | Burundi           |             40.9382 |
#  
#  ------------------------------
#  