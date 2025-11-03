from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def custom_string_funcion(df:DataFrame): 
    df['anio'] = df['anios_observados'].str.split(' - ').str[0]
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share_trabajo_no_remun', k=100),
	custom_string_funcion(),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'anios_observados'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         85 non-null     object 
#   1   geonombreFundar         85 non-null     object 
#   2   continente_fundar       85 non-null     object 
#   3   anios_observados        85 non-null     object 
#   4   share_trabajo_no_remun  85 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|-------------------------:|
#  |  0 | ALB               | Albania           | Europa              | 2010 - 2011        |                 0.857923 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share_trabajo_no_remun', k=100)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         85 non-null     object 
#   1   geonombreFundar         85 non-null     object 
#   2   continente_fundar       85 non-null     object 
#   3   anios_observados        85 non-null     object 
#   4   share_trabajo_no_remun  85 non-null     float64
#   5   anio                    85 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |   anio |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|-------------------------:|-------:|
#  |  0 | ALB               | Albania           | Europa              | 2010 - 2011        |                  85.7923 |   2010 |
#  
#  ------------------------------
#  
#  custom_string_funcion()
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         85 non-null     object 
#   1   geonombreFundar         85 non-null     object 
#   2   continente_fundar       85 non-null     object 
#   3   anios_observados        85 non-null     object 
#   4   share_trabajo_no_remun  85 non-null     float64
#   5   anio                    85 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |   anio |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|-------------------------:|-------:|
#  |  0 | ALB               | Albania           | Europa              | 2010 - 2011        |                  85.7923 |   2010 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'anios_observados'], axis=1)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         85 non-null     object 
#   1   share_trabajo_no_remun  85 non-null     float64
#   2   anio                    85 non-null     object 
#  
#  |    | geonombreFundar   |   share_trabajo_no_remun |   anio |
#  |---:|:------------------|-------------------------:|-------:|
#  |  0 | Albania           |                  85.7923 |   2010 |
#  
#  ------------------------------
#  