from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='geocodigoFundar != "F351" ')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  193 non-null    object 
#   1   geonombreFundar  193 non-null    object 
#   2   valor            193 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | AFG               | Afganistán        | 86716.5 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar != "F351" ')
#  Index: 193 entries, 0 to 192
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  193 non-null    object 
#   1   geonombreFundar  193 non-null    object 
#   2   valor            193 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | AFG               | Afganistán        | 86716.5 |
#  
#  ------------------------------
#  