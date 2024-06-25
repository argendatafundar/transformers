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
rename_cols(map={'empresas': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 27 entries, 0 to 26
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype
#  ---  ------    --------------  -----
#   0   anio      27 non-null     int64
#   1   empresas  27 non-null     int64
#  
#  |    |   anio |   empresas |
#  |---:|-------:|-----------:|
#  |  0 |   1996 |     408982 |
#  
#  ------------------------------
#  
#  rename_cols(map={'empresas': 'valor'})
#  RangeIndex: 27 entries, 0 to 26
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype
#  ---  ------  --------------  -----
#   0   anio    27 non-null     int64
#   1   valor   27 non-null     int64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1996 |  408982 |
#  
#  ------------------------------
#  