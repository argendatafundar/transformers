from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='finalidad_funcion', curr_value='GASTO PÚBLICO TOTAL', new_value='Total'),
	multiplicar_por_escalar(col='participacion_en_el_gasto_publico_consolidado', k=100),
	query(condition="finalidad_funcion != 'SERVICIOS DE LA DEUDA PÚBLICA'"),
	ordenar_dos_columnas(col1='finalidad_funcion', order1=['Ciencia y técnica', 'Energía y combustible', 'Trabajo', 'Previsión social', 'Educación superior y universitaria', 'Promoción y asistencia social', 'Agua potable y alcantarillado', 'Total', 'Servicios', 'Salud', 'Industria', 'Defensa y seguridad', 'Producción primaria', 'Justicia', 'Administración general', 'Cultura', 'Educación y cultura sin discriminar', 'Otros gastos en servicios económicos', 'Vivienda y urbanismo', 'Educación básica', 'Otros servicios urbanos'], col2='nivel_de_gobierno', order2=['Nacional', 'Provincial', 'Municipal']),
	query(condition="finalidad_funcion != 'Educación y cultura sin discriminar'"),
	replace_multiple_values(col='finalidad_funcion', replacements={'Educación superior y universitaria': 'Educación superior', 'Promoción y asistencia social': 'Asistencia social', 'Otros gastos en servicios económicos': 'Otros serv. económicos', 'Otros servicios urbanos': 'Servicios urbanos', 'Administración general': 'Adm. general', 'Agua potable y alcantarillado': 'Agua y alcantarillado'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype  
#  ---  ------                                         --------------  -----  
#   0   anio                                           66 non-null     int64  
#   1   nivel_de_gobierno                              66 non-null     object 
#   2   codigo                                         66 non-null     object 
#   3   finalidad_funcion                              66 non-null     object 
#   4   gasto_publico_porcenataje_del_pib              66 non-null     float64
#   5   participacion_en_el_gasto_publico_consolidado  66 non-null     float64
#  
#  |    |   anio | nivel_de_gobierno   |   codigo | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|---------:|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  0 |   2023 | Nacional            |        1 | GASTO PÚBLICO TOTAL |                             22.2951 |                                        0.532529 |
#  
#  ------------------------------
#  
#  replace_value(col='finalidad_funcion', curr_value='GASTO PÚBLICO TOTAL', new_value='Total')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype  
#  ---  ------                                         --------------  -----  
#   0   anio                                           66 non-null     int64  
#   1   nivel_de_gobierno                              66 non-null     object 
#   2   codigo                                         66 non-null     object 
#   3   finalidad_funcion                              66 non-null     object 
#   4   gasto_publico_porcenataje_del_pib              66 non-null     float64
#   5   participacion_en_el_gasto_publico_consolidado  66 non-null     float64
#  
#  |    |   anio | nivel_de_gobierno   |   codigo | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|---------:|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  0 |   2023 | Nacional            |        1 | Total               |                             22.2951 |                                        0.532529 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='participacion_en_el_gasto_publico_consolidado', k=100)
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype  
#  ---  ------                                         --------------  -----  
#   0   anio                                           66 non-null     int64  
#   1   nivel_de_gobierno                              66 non-null     object 
#   2   codigo                                         66 non-null     object 
#   3   finalidad_funcion                              66 non-null     object 
#   4   gasto_publico_porcenataje_del_pib              66 non-null     float64
#   5   participacion_en_el_gasto_publico_consolidado  66 non-null     float64
#  
#  |    |   anio | nivel_de_gobierno   |   codigo | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|---------:|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  0 |   2023 | Nacional            |        1 | Total               |                             22.2951 |                                         53.2529 |
#  
#  ------------------------------
#  
#  query(condition="finalidad_funcion != 'SERVICIOS DE LA DEUDA PÚBLICA'")
#  Index: 63 entries, 0 to 64
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype   
#  ---  ------                                         --------------  -----   
#   0   anio                                           63 non-null     int64   
#   1   nivel_de_gobierno                              63 non-null     category
#   2   codigo                                         63 non-null     object  
#   3   finalidad_funcion                              63 non-null     category
#   4   gasto_publico_porcenataje_del_pib              63 non-null     float64 
#   5   participacion_en_el_gasto_publico_consolidado  63 non-null     float64 
#  
#  |    |   anio | nivel_de_gobierno   |   codigo | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|---------:|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  0 |   2023 | Nacional            |        1 | Total               |                             22.2951 |                                         53.2529 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='finalidad_funcion', order1=['Ciencia y técnica', 'Energía y combustible', 'Trabajo', 'Previsión social', 'Educación superior y universitaria', 'Promoción y asistencia social', 'Agua potable y alcantarillado', 'Total', 'Servicios', 'Salud', 'Industria', 'Defensa y seguridad', 'Producción primaria', 'Justicia', 'Administración general', 'Cultura', 'Educación y cultura sin discriminar', 'Otros gastos en servicios económicos', 'Vivienda y urbanismo', 'Educación básica', 'Otros servicios urbanos'], col2='nivel_de_gobierno', order2=['Nacional', 'Provincial', 'Municipal'])
#  Index: 63 entries, 6 to 59
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype   
#  ---  ------                                         --------------  -----   
#   0   anio                                           63 non-null     int64   
#   1   nivel_de_gobierno                              63 non-null     category
#   2   codigo                                         63 non-null     object  
#   3   finalidad_funcion                              63 non-null     category
#   4   gasto_publico_porcenataje_del_pib              63 non-null     float64 
#   5   participacion_en_el_gasto_publico_consolidado  63 non-null     float64 
#  
#  |    |   anio | nivel_de_gobierno   | codigo   | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|:---------|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  6 |   2023 | Nacional            | 1.2.1.3  | Ciencia y técnica   |                            0.243942 |                                          91.233 |
#  
#  ------------------------------
#  
#  query(condition="finalidad_funcion != 'Educación y cultura sin discriminar'")
#  Index: 60 entries, 6 to 59
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype   
#  ---  ------                                         --------------  -----   
#   0   anio                                           60 non-null     int64   
#   1   nivel_de_gobierno                              60 non-null     category
#   2   codigo                                         60 non-null     object  
#   3   finalidad_funcion                              60 non-null     category
#   4   gasto_publico_porcenataje_del_pib              60 non-null     float64 
#   5   participacion_en_el_gasto_publico_consolidado  60 non-null     float64 
#  
#  |    |   anio | nivel_de_gobierno   | codigo   | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|:---------|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  6 |   2023 | Nacional            | 1.2.1.3  | Ciencia y técnica   |                            0.243942 |                                          91.233 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='finalidad_funcion', replacements={'Educación superior y universitaria': 'Educación superior', 'Promoción y asistencia social': 'Asistencia social', 'Otros gastos en servicios económicos': 'Otros serv. económicos', 'Otros servicios urbanos': 'Servicios urbanos', 'Administración general': 'Adm. general', 'Agua potable y alcantarillado': 'Agua y alcantarillado'})
#  Index: 60 entries, 6 to 59
#  Data columns (total 6 columns):
#   #   Column                                         Non-Null Count  Dtype   
#  ---  ------                                         --------------  -----   
#   0   anio                                           60 non-null     int64   
#   1   nivel_de_gobierno                              60 non-null     category
#   2   codigo                                         60 non-null     object  
#   3   finalidad_funcion                              60 non-null     category
#   4   gasto_publico_porcenataje_del_pib              60 non-null     float64 
#   5   participacion_en_el_gasto_publico_consolidado  60 non-null     float64 
#  
#  |    |   anio | nivel_de_gobierno   | codigo   | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|:---------|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  6 |   2023 | Nacional            | 1.2.1.3  | Ciencia y técnica   |                            0.243942 |                                          91.233 |
#  
#  ------------------------------
#  