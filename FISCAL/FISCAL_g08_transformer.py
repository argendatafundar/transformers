from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='finalidad_funcion', curr_value='GASTO PÚBLICO TOTAL', new_value='Total'),
	multiplicar_por_escalar(col='participacion_en_el_gasto_publico_consolidado', k=100),
	query(condition="finalidad_funcion != 'SERVICIOS DE LA DEUDA PÚBLICA'")
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
#  |  0 |   2023 | Nacional            |        1 | Total               |                             22.2951 |                                         53.2529 |
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
#   1   nivel_de_gobierno                              63 non-null     object 
#   2   codigo                                         63 non-null     object 
#   3   finalidad_funcion                              63 non-null     object 
#   4   gasto_publico_porcenataje_del_pib              63 non-null     float64
#   5   participacion_en_el_gasto_publico_consolidado  63 non-null     float64
#  
#  |    |   anio | nivel_de_gobierno   |   codigo | finalidad_funcion   |   gasto_publico_porcenataje_del_pib |   participacion_en_el_gasto_publico_consolidado |
#  |---:|-------:|:--------------------|---------:|:--------------------|------------------------------------:|------------------------------------------------:|
#  |  0 |   2023 | Nacional            |        1 | Total               |                             22.2951 |                                         53.2529 |
#  
#  ------------------------------
#  