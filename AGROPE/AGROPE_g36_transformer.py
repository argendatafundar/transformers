from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_cadena': 'categoria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |    0.86 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_cadena': 'categoria'})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Miel        |    0.86 |
#  
#  ------------------------------
#  