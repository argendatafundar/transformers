from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais_desc', axis=1),
	drop_col(col='continente_fundar', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'share_trabajo_no_remun': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    85 non-null     object 
#   1   pais_desc               85 non-null     object 
#   2   continente_fundar       85 non-null     object 
#   3   anios_observados        85 non-null     object 
#   4   share_trabajo_no_remun  85 non-null     float64
#  
#  |    | iso3   | pais_desc   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:-------|:------------|:--------------------|:-------------------|-------------------------:|
#  |  0 | ALB    | Albania     | Europa              | 2010 - 2011        |                 0.857923 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    85 non-null     object 
#   1   continente_fundar       85 non-null     object 
#   2   anios_observados        85 non-null     object 
#   3   share_trabajo_no_remun  85 non-null     float64
#  
#  |    | iso3   | continente_fundar   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:-------|:--------------------|:-------------------|-------------------------:|
#  |  0 | ALB    | Europa              | 2010 - 2011        |                 0.857923 |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    85 non-null     object 
#   1   anios_observados        85 non-null     object 
#   2   share_trabajo_no_remun  85 non-null     float64
#  
#  |    | iso3   | anios_observados   |   share_trabajo_no_remun |
#  |---:|:-------|:-------------------|-------------------------:|
#  |  0 | ALB    | 2010 - 2011        |                 0.857923 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'share_trabajo_no_remun': 'valor'})
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         85 non-null     object 
#   1   anios_observados  85 non-null     object 
#   2   valor             85 non-null     float64
#  
#  |    | geocodigo   | anios_observados   |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | ALB         | 2010 - 2011        | 85.7923 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         85 non-null     object 
#   1   anios_observados  85 non-null     object 
#   2   valor             85 non-null     float64
#  
#  |    | geocodigo   | anios_observados   |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | ALB         | 2010 - 2011        | 85.7923 |
#  
#  ------------------------------
#  