from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='funciones', replacements={'Educación, cultura y ciencia y técnica': 'Educación, cultura y ciencia', 'Salud': 'Salud', 'Agua potable y alcantarillado': 'Agua y alcantarillado', 'Vivienda y urbanismo': 'Vivienda y urbanismo', 'Promoción y asistencia social': 'Asistencia social', 'Previsión social': 'Previsión social', 'Trabajo': 'Trabajo', 'Otros servicios urbanos': 'Servicios urbanos'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 352 entries, 0 to 351
#  Data columns (total 4 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   anio                              352 non-null    int64  
#   1   codigo                            352 non-null    object 
#   2   funciones                         352 non-null    object 
#   3   gasto_publico_social_consolidado  352 non-null    float64
#  
#  |    |   anio | codigo   | funciones                              |   gasto_publico_social_consolidado |
#  |---:|-------:|:---------|:---------------------------------------|-----------------------------------:|
#  |  0 |   1980 | 1.2.1    | Educación, cultura y ciencia y técnica |                             2.9878 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='funciones', replacements={'Educación, cultura y ciencia y técnica': 'Educación, cultura y ciencia', 'Salud': 'Salud', 'Agua potable y alcantarillado': 'Agua y alcantarillado', 'Vivienda y urbanismo': 'Vivienda y urbanismo', 'Promoción y asistencia social': 'Asistencia social', 'Previsión social': 'Previsión social', 'Trabajo': 'Trabajo', 'Otros servicios urbanos': 'Servicios urbanos'})
#  RangeIndex: 352 entries, 0 to 351
#  Data columns (total 4 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   anio                              352 non-null    int64  
#   1   codigo                            352 non-null    object 
#   2   funciones                         352 non-null    object 
#   3   gasto_publico_social_consolidado  352 non-null    float64
#  
#  |    |   anio | codigo   | funciones                    |   gasto_publico_social_consolidado |
#  |---:|-------:|:---------|:-----------------------------|-----------------------------------:|
#  |  0 |   1980 | 1.2.1    | Educación, cultura y ciencia |                             2.9878 |
#  
#  ------------------------------
#  