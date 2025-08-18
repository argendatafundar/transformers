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
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 |
#  
#  ------------------------------
#  