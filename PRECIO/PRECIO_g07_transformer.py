from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(pais_o_grupo_de_paises='grupo', mediana_paises_inflacion='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      64 non-null     int64  
#   1   mediana_paises_inflacion  64 non-null     float64
#   2   pais_o_grupo_de_paises    64 non-null     object 
#  
#  |    |   anio |   mediana_paises_inflacion | pais_o_grupo_de_paises   |
#  |---:|-------:|---------------------------:|:-------------------------|
#  |  0 |   1992 |                    17.5459 | Argentina                |
#  
#  ------------------------------
#  
#  rename_columns(pais_o_grupo_de_paises='grupo', mediana_paises_inflacion='valor')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    64 non-null     int64  
#   1   valor   64 non-null     float64
#   2   grupo   64 non-null     object 
#  
#  |    |   anio |   valor | grupo     |
#  |---:|-------:|--------:|:----------|
#  |  0 |   1992 | 17.5459 | Argentina |
#  
#  ------------------------------
#  