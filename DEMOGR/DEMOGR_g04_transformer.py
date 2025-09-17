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
	query(condition='anio <= 2025')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 468 entries, 0 to 467
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        468 non-null    int64  
#   1   grupo_edad  468 non-null    object 
#   2   poblacion   468 non-null    int64  
#   3   share       468 non-null    float64
#  
#  |    |   anio | grupo_edad   |   poblacion |   share |
#  |---:|-------:|:-------------|------------:|--------:|
#  |  0 |   1869 | 20 a 64      |      777704 | 44.7748 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 240 entries, 0 to 239
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        240 non-null    int64  
#   1   grupo_edad  240 non-null    object 
#   2   poblacion   240 non-null    int64  
#   3   share       240 non-null    float64
#  
#  |    |   anio | grupo_edad   |   poblacion |   share |
#  |---:|-------:|:-------------|------------:|--------:|
#  |  0 |   1869 | 20 a 64      |      777704 | 44.7748 |
#  
#  ------------------------------
#  