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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais_desc', axis=1),
	drop_col(col='continente_fundar', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'share_trabajo_no_remun': 'valor'})
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
#   3   share_trabajo_no_remun  85 non-null     float64
#   4   anios_observados        85 non-null     object 
#  
#  |    | iso3   | pais_desc   | continente_fundar   |   share_trabajo_no_remun | anios_observados   |
#  |---:|:-------|:------------|:--------------------|-------------------------:|:-------------------|
#  |  0 | ALB    | Albania     | Europa              |                 0.857924 | 2010 - 2011        |
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
#   2   share_trabajo_no_remun  85 non-null     float64
#   3   anios_observados        85 non-null     object 
#  
#  |    | iso3   | continente_fundar   |   share_trabajo_no_remun | anios_observados   |
#  |---:|:-------|:--------------------|-------------------------:|:-------------------|
#  |  0 | ALB    | Europa              |                 0.857924 | 2010 - 2011        |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    85 non-null     object 
#   1   share_trabajo_no_remun  85 non-null     float64
#   2   anios_observados        85 non-null     object 
#  
#  |    | iso3   |   share_trabajo_no_remun | anios_observados   |
#  |---:|:-------|-------------------------:|:-------------------|
#  |  0 | ALB    |                 0.857924 | 2010 - 2011        |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'share_trabajo_no_remun': 'valor'})
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         85 non-null     object 
#   1   valor             85 non-null     float64
#   2   anios_observados  85 non-null     object 
#  
#  |    | geocodigo   |    valor | anios_observados   |
#  |---:|:------------|---------:|:-------------------|
#  |  0 | ALB         | 0.857924 | 2010 - 2011        |
#  
#  ------------------------------
#  