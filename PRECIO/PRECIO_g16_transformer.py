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
rename_cols(map={'tasa_inflacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            88 non-null     int64  
#   1   tasa_inflacion  88 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tasa_inflacion': 'valor'})
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    88 non-null     int64  
#   1   valor   88 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1935 | 14.0881 |
#  
#  ------------------------------
#  