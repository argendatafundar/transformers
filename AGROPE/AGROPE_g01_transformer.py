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
rename_cols(map={'participacion_agro': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                89 non-null     int64  
#   1   participacion_agro  89 non-null     float64
#  
#  |    |   anio |   participacion_agro |
#  |---:|-------:|---------------------:|
#  |  0 |   1935 |             0.206776 |
#  
#  ------------------------------
#  
#  rename_cols(map={'participacion_agro': 'valor'})
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    89 non-null     int64  
#   1   valor   89 non-null     float64
#  
#  |    |   anio |    valor |
#  |---:|-------:|---------:|
#  |  0 |   1935 | 0.206776 |
#  
#  ------------------------------
#  