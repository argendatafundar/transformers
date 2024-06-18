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
#  RangeIndex: 34 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          34 non-null     int64  
#   1   sector        34 non-null     object 
#   2   prop_mujeres  34 non-null     float64
#  
#  |    |   anio | sector   |   prop_mujeres |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2007 | SBC      |       0.384661 |
#  
#  ------------------------------
#  
#  rename_columns(sector='categoria', prop_mujeres='valor')
#  RangeIndex: 34 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       34 non-null     int64  
#   1   categoria  34 non-null     object 
#   2   valor      34 non-null     float64
#  
#  |    |   anio | categoria   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   2007 | SBC         | 0.384661 |
#  
#  ------------------------------
#  