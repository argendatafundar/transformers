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
rename_cols(map={'participacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           24 non-null     int64  
#   1   categoria      24 non-null     object 
#   2   participacion  24 non-null     float64
#  
#  |    |   anio | categoria                          |   participacion |
#  |---:|-------:|:-----------------------------------|----------------:|
#  |  0 |   2016 | Remuneración al trabajo asalariado |         51.8371 |
#  
#  ------------------------------
#  
#  rename_cols(map={'participacion': 'valor'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       24 non-null     int64  
#   1   categoria  24 non-null     object 
#   2   valor      24 non-null     float64
#  
#  |    |   anio | categoria                          |   valor |
#  |---:|-------:|:-----------------------------------|--------:|
#  |  0 |   2016 | Remuneración al trabajo asalariado | 51.8371 |
#  
#  ------------------------------
#  