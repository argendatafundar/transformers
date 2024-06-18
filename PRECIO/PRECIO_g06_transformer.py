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
rename_columns(grupo_de_paises='grupo', mediana_paises_inflacion='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   grupo_de_paises           88 non-null     object 
#   1   anio                      88 non-null     int64  
#   2   mediana_paises_inflacion  88 non-null     float64
#  
#  |    | grupo_de_paises         |   anio |   mediana_paises_inflacion |
#  |---:|:------------------------|-------:|---------------------------:|
#  |  0 | América Latina y Caribe |   1980 |                     21.206 |
#  
#  ------------------------------
#  
#  rename_columns(grupo_de_paises='grupo', mediana_paises_inflacion='valor')
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   grupo   88 non-null     object 
#   1   anio    88 non-null     int64  
#   2   valor   88 non-null     float64
#  
#  |    | grupo                   |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | América Latina y Caribe |   1980 |  21.206 |
#  
#  ------------------------------
#  