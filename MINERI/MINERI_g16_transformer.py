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
rename_columns(sector='categoria', tasa_feminizacion='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   sector             14 non-null     object 
#   1   tasa_feminizacion  14 non-null     float64
#  
#  |    | sector    |   tasa_feminizacion |
#  |---:|:----------|--------------------:|
#  |  0 | Enseñanza |                72.8 |
#  
#  ------------------------------
#  
#  rename_columns(sector='categoria', tasa_feminizacion='valor')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   valor      14 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Enseñanza   |    72.8 |
#  
#  ------------------------------
#  