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
#  RangeIndex: 231 entries, 0 to 230
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype 
#  ---  ------      --------------  ----- 
#   0   anio        231 non-null    int64 
#   1   grupo_edad  231 non-null    object
#   2   poblacion   231 non-null    int64 
#  
#  |    |   anio | grupo_edad   |   poblacion |
#  |---:|-------:|:-------------|------------:|
#  |  0 |   2025 | 65 y más     |     5733401 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 231 entries, 0 to 230
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype 
#  ---  ------      --------------  ----- 
#   0   anio        231 non-null    int64 
#   1   grupo_edad  231 non-null    object
#   2   poblacion   231 non-null    int64 
#  
#  |    |   anio | grupo_edad   |   poblacion |
#  |---:|-------:|:-------------|------------:|
#  |  0 |   2025 | 65 y más     |     5733401 |
#  
#  ------------------------------
#  