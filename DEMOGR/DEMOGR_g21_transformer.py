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
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       148 non-null    int64 
#   1   categoria  148 non-null    object
#   2   poblacion  148 non-null    int64 
#  
#  |    |   anio | categoria          |   poblacion |
#  |---:|-------:|:-------------------|------------:|
#  |  0 |   1950 | Varones de 65 años |       38677 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       148 non-null    int64 
#   1   categoria  148 non-null    object
#   2   poblacion  148 non-null    int64 
#  
#  |    |   anio | categoria          |   poblacion |
#  |---:|-------:|:-------------------|------------:|
#  |  0 |   1950 | Varones de 65 años |       38677 |
#  
#  ------------------------------
#  