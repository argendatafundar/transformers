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
rename_columns(sector='categoria', prop_mujeres='valor')
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
#  rename_columns(sector='categoria', prop_mujeres='valor')
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   categoria  54 non-null     object 
#   2   valor      54 non-null     float64
#  
#  |    |   anio | categoria         |    valor |
#  |---:|-------:|:------------------|---------:|
#  |  0 |   1996 | Promedio economia | 0.267976 |
#  
#  ------------------------------
#  