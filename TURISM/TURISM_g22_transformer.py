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
#  RangeIndex: 235 entries, 0 to 234
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype  
#  ---  ------                                     --------------  -----  
#   0   anio                                       235 non-null    int64  
#   1   geocodigoFundar                            235 non-null    object 
#   2   geonombreFundar                            235 non-null    object 
#   3   arribos_turisticos_internacionales_millon  235 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turisticos_internacionales_millon |
#  |---:|-------:|:------------------|:------------------|--------------------------------------------:|
#  |  0 |   2024 | ZEUR              | Europa            |                                       758.6 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 235 entries, 0 to 234
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype  
#  ---  ------                                     --------------  -----  
#   0   anio                                       235 non-null    int64  
#   1   geocodigoFundar                            235 non-null    object 
#   2   geonombreFundar                            235 non-null    object 
#   3   arribos_turisticos_internacionales_millon  235 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turisticos_internacionales_millon |
#  |---:|-------:|:------------------|:------------------|--------------------------------------------:|
#  |  0 |   2024 | ZEUR              | Europa            |                                       758.6 |
#  
#  ------------------------------
#  