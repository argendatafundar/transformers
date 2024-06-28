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
rename_cols(map={'sexo': 'categoria', 'tasa_participacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                33 non-null     int64  
#   1   sexo                33 non-null     object 
#   2   tasa_participacion  33 non-null     float64
#  
#  |    |   anio | sexo    |   tasa_participacion |
#  |---:|-------:|:--------|---------------------:|
#  |  0 |   1869 | Varones |             0.906793 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'categoria', 'tasa_participacion': 'valor'})
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       33 non-null     int64  
#   1   categoria  33 non-null     object 
#   2   valor      33 non-null     float64
#  
#  |    |   anio | categoria   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1869 | Varones     | 0.906793 |
#  
#  ------------------------------
#  