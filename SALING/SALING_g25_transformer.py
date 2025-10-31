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
#  RangeIndex: 1717 entries, 0 to 1716
#  Data columns (total 4 columns):
#   #   Column                                 Non-Null Count  Dtype  
#  ---  ------                                 --------------  -----  
#   0   geocodigoFundar                        1717 non-null   object 
#   1   geonombreFundar                        1717 non-null   object 
#   2   anio                                   1717 non-null   int64  
#   3   salario_real_ppa_consumo_privado_2017  1717 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   salario_real_ppa_consumo_privado_2017 |
#  |---:|:------------------|:------------------|-------:|----------------------------------------:|
#  |  0 | USA               | Estados Unidos    |   1929 |                                 1881.73 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 1717 entries, 0 to 1716
#  Data columns (total 4 columns):
#   #   Column                                 Non-Null Count  Dtype  
#  ---  ------                                 --------------  -----  
#   0   geocodigoFundar                        1717 non-null   object 
#   1   geonombreFundar                        1717 non-null   object 
#   2   anio                                   1717 non-null   int64  
#   3   salario_real_ppa_consumo_privado_2017  1717 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   salario_real_ppa_consumo_privado_2017 |
#  |---:|:------------------|:------------------|-------:|----------------------------------------:|
#  |  0 | USA               | Estados Unidos    |   1929 |                                 1881.73 |
#  
#  ------------------------------
#  