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
rename_columns(tasa_inflacion='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            89 non-null     int64  
#   1   tasa_inflacion  89 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  
#  rename_columns(tasa_inflacion='valor')
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    89 non-null     int64  
#   1   valor   89 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1935 | 14.0881 |
#  
#  ------------------------------
#  