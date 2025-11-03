from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	query(condition="anios_observados == '2021 - 2021'"),
	multiplicar_por_escalar(col='share_trabajo_no_remun', k=100)
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
#  query(condition="anios_observados == '2021 - 2021'")
#  Index: 5 entries, 2 to 70
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         5 non-null      object 
#   1   geonombreFundar         5 non-null      object 
#   2   continente_fundar       5 non-null      object 
#   3   anios_observados        5 non-null      object 
#   4   share_trabajo_no_remun  5 non-null      float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|-------------------------:|
#  |  2 | ARG               | Argentina         | América del Sur     | 2021 - 2021        |                  66.3317 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share_trabajo_no_remun', k=100)
#  Index: 5 entries, 2 to 70
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         5 non-null      object 
#   1   geonombreFundar         5 non-null      object 
#   2   continente_fundar       5 non-null      object 
#   3   anios_observados        5 non-null      object 
#   4   share_trabajo_no_remun  5 non-null      float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|-------------------------:|
#  |  2 | ARG               | Argentina         | América del Sur     | 2021 - 2021        |                  66.3317 |
#  
#  ------------------------------
#  