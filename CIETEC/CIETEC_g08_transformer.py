from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7 entries, 0 to 6
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     7 non-null      int64  
#   1   disciplina_de_formacion  7 non-null      object 
#   2   personas_fisicas         7 non-null      int64  
#   3   share                    7 non-null      float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |     share |
#  |---:|-------:|:----------------------------------|-------------------:|----------:|
#  |  0 |   2023 | Ciencias Agrícolas y Veterinarias |               9203 | 0.0934049 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share', k=100)
#  RangeIndex: 7 entries, 0 to 6
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     7 non-null      int64  
#   1   disciplina_de_formacion  7 non-null      object 
#   2   personas_fisicas         7 non-null      int64  
#   3   share                    7 non-null      float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |   share |
#  |---:|-------:|:----------------------------------|-------------------:|--------:|
#  |  0 |   2023 | Ciencias Agrícolas y Veterinarias |               9203 | 9.34049 |
#  
#  ------------------------------
#  