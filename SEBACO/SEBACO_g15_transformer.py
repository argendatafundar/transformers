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
rename_cols(map={'sector': 'indicador'}),
	rename_cols(map={'prop_mujeres': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          54 non-null     int64  
#   1   sector        54 non-null     object 
#   2   prop_mujeres  54 non-null     float64
#  
#  |    |   anio | sector            |   prop_mujeres |
#  |---:|-------:|:------------------|---------------:|
#  |  0 |   1996 | Promedio economia |       0.267976 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          54 non-null     int64  
#   1   indicador     54 non-null     object 
#   2   prop_mujeres  54 non-null     float64
#  
#  |    |   anio | indicador         |   prop_mujeres |
#  |---:|-------:|:------------------|---------------:|
#  |  0 |   1996 | Promedio economia |       0.267976 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_mujeres': 'valor'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   indicador  54 non-null     object 
#   2   valor      54 non-null     float64
#  
#  |    |   anio | indicador         |    valor |
#  |---:|-------:|:------------------|---------:|
#  |  0 |   1996 | Promedio economia | 0.267976 |
#  
#  ------------------------------
#  