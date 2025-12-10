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
#  RangeIndex: 191 entries, 0 to 190
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   geocodigoFundar          191 non-null    object 
#   1   geonombreFundar          191 non-null    object 
#   2   ratio_emisivo_ponderado  191 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ratio_emisivo_ponderado |
#  |---:|:------------------|:------------------|--------------------------:|
#  |  0 | ABW               | Aruba             |                   16.7893 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 191 entries, 0 to 190
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   geocodigoFundar          191 non-null    object 
#   1   geonombreFundar          191 non-null    object 
#   2   ratio_emisivo_ponderado  191 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ratio_emisivo_ponderado |
#  |---:|:------------------|:------------------|--------------------------:|
#  |  0 | ABW               | Aruba             |                   16.7893 |
#  
#  ------------------------------
#  