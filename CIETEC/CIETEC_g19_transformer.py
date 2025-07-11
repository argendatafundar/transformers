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
	query(condition='anio in [2009, 2016, 2023]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     105 non-null    int64  
#   1   disciplina_de_formacion  105 non-null    object 
#   2   personas_fisicas         105 non-null    int64  
#   3   share                    105 non-null    float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |   share |
#  |---:|-------:|:----------------------------------|-------------------:|--------:|
#  |  0 |   2009 | Ciencias Agrícolas y Veterinarias |               7461 | 11.5295 |
#  
#  ------------------------------
#  
#  query(condition='anio in [2009, 2016, 2023]')
#  Index: 21 entries, 0 to 104
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     21 non-null     int64  
#   1   disciplina_de_formacion  21 non-null     object 
#   2   personas_fisicas         21 non-null     int64  
#   3   share                    21 non-null     float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |   share |
#  |---:|-------:|:----------------------------------|-------------------:|--------:|
#  |  0 |   2009 | Ciencias Agrícolas y Veterinarias |               7461 | 11.5295 |
#  
#  ------------------------------
#  