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
rename_cols(map={'pais_o_grupo_de_paises': 'indicador'}),
	rename_cols(map={'mediana_paises_inflacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   pais_o_grupo_de_paises    62 non-null     object 
#   1   anio                      62 non-null     int64  
#   2   mediana_paises_inflacion  62 non-null     float64
#  
#  |    | pais_o_grupo_de_paises   |   anio |   mediana_paises_inflacion |
#  |---:|:-------------------------|-------:|---------------------------:|
#  |  0 | Argentina                |   1992 |                      17.55 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pais_o_grupo_de_paises': 'indicador'})
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   indicador                 62 non-null     object 
#   1   anio                      62 non-null     int64  
#   2   mediana_paises_inflacion  62 non-null     float64
#  
#  |    | indicador   |   anio |   mediana_paises_inflacion |
#  |---:|:------------|-------:|---------------------------:|
#  |  0 | Argentina   |   1992 |                      17.55 |
#  
#  ------------------------------
#  
#  rename_cols(map={'mediana_paises_inflacion': 'valor'})
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  62 non-null     object 
#   1   anio       62 non-null     int64  
#   2   valor      62 non-null     float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   1992 |   17.55 |
#  
#  ------------------------------
#  