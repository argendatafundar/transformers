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
rename_cols(map={'ano': 'anio', 'gini': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 51 entries, 0 to 50
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   ano     51 non-null     int64  
#   1   gini    51 non-null     float64
#  
#  |    |   ano |   gini |
#  |---:|------:|-------:|
#  |  0 |  1974 |   34.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'gini': 'valor'})
#  RangeIndex: 51 entries, 0 to 50
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    51 non-null     int64  
#   1   valor   51 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1974 |    34.8 |
#  
#  ------------------------------
#  