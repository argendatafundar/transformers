from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	multiplicar_por_escalar(col='tasa_informalidad_legal', k=100),
	replace_multiple_values(col='apertura_sexo', replacements={'femenino': 'Mujeres', 'masculino': 'Mujeres', 'total': 'Total'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1272 entries, 0 to 1271
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     1272 non-null   int64  
#   1   edad                     1272 non-null   int64  
#   2   apertura_sexo            1272 non-null   object 
#   3   tasa_informalidad_legal  1272 non-null   float64
#  
#  |    |   anio |   edad | apertura_sexo   |   tasa_informalidad_legal |
#  |---:|-------:|-------:|:----------------|--------------------------:|
#  |  0 |   2016 |     18 | femenino        |                  0.945639 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 159 entries, 1113 to 1271
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     159 non-null    int64  
#   1   edad                     159 non-null    int64  
#   2   apertura_sexo            159 non-null    object 
#   3   tasa_informalidad_legal  159 non-null    float64
#  
#  |      |   anio |   edad | apertura_sexo   |   tasa_informalidad_legal |
#  |-----:|-------:|-------:|:----------------|--------------------------:|
#  | 1113 |   2023 |     18 | femenino        |                   89.7501 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_informalidad_legal', k=100)
#  Index: 159 entries, 1113 to 1271
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     159 non-null    int64  
#   1   edad                     159 non-null    int64  
#   2   apertura_sexo            159 non-null    object 
#   3   tasa_informalidad_legal  159 non-null    float64
#  
#  |      |   anio |   edad | apertura_sexo   |   tasa_informalidad_legal |
#  |-----:|-------:|-------:|:----------------|--------------------------:|
#  | 1113 |   2023 |     18 | femenino        |                   89.7501 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='apertura_sexo', replacements={'femenino': 'Mujeres', 'masculino': 'Mujeres', 'total': 'Total'})
#  Index: 159 entries, 1113 to 1271
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     159 non-null    int64  
#   1   edad                     159 non-null    int64  
#   2   apertura_sexo            159 non-null    object 
#   3   tasa_informalidad_legal  159 non-null    float64
#  
#  |      |   anio |   edad | apertura_sexo   |   tasa_informalidad_legal |
#  |-----:|-------:|-------:|:----------------|--------------------------:|
#  | 1113 |   2023 |     18 | Mujeres         |                   89.7501 |
#  
#  ------------------------------
#  