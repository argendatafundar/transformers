from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'grupo_de_paises': 'indicador'}),
	rename_cols(map={'mediana_paises_inflacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 86 entries, 0 to 85
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   grupo_de_paises           86 non-null     object 
#   1   anio                      86 non-null     int64  
#   2   mediana_paises_inflacion  86 non-null     float64
#  
#  |    | grupo_de_paises         |   anio |   mediana_paises_inflacion |
#  |---:|:------------------------|-------:|---------------------------:|
#  |  0 | América Latina y Caribe |   1980 |                     21.206 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_de_paises': 'indicador'})
#  RangeIndex: 86 entries, 0 to 85
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   indicador                 86 non-null     object 
#   1   anio                      86 non-null     int64  
#   2   mediana_paises_inflacion  86 non-null     float64
#  
#  |    | indicador               |   anio |   mediana_paises_inflacion |
#  |---:|:------------------------|-------:|---------------------------:|
#  |  0 | América Latina y Caribe |   1980 |                     21.206 |
#  
#  ------------------------------
#  
#  rename_cols(map={'mediana_paises_inflacion': 'valor'})
#  RangeIndex: 86 entries, 0 to 85
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  86 non-null     object 
#   1   anio       86 non-null     int64  
#   2   valor      86 non-null     float64
#  
#  |    | indicador               |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | América Latina y Caribe |   1980 |  21.206 |
#  
#  ------------------------------
#  