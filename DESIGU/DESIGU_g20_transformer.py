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
rename_cols(map={'ano': 'anio', 'indice': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   ano     43 non-null     int64  
#   1   indice  43 non-null     float64
#  
#  |    |   ano |   indice |
#  |---:|------:|---------:|
#  |  0 |  1980 |   0.6285 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'indice': 'valor'})
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    43 non-null     int64  
#   1   valor   43 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1980 |  0.6285 |
#  
#  ------------------------------
#  