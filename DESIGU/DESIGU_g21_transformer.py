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
rename_cols(map={'ano': 'anio', 'brecha': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   ano     22 non-null     int64  
#   1   brecha  22 non-null     float64
#  
#  |    |   ano |   brecha |
#  |---:|------:|---------:|
#  |  0 |  2000 |     0.14 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'brecha': 'valor'})
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    22 non-null     int64  
#   1   valor   22 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2000 |    0.14 |
#  
#  ------------------------------
#  