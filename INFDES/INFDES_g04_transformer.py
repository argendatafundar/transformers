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
rename_cols(map={'tipo_informalidad': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 65 entries, 0 to 64
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               65 non-null     int64  
#   1   tipo_informalidad  65 non-null     object 
#   2   valor              65 non-null     float64
#  
#  |    |   anio | tipo_informalidad   |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2021 | Definición legal    | 32.7077 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_informalidad': 'indicador'})
#  RangeIndex: 65 entries, 0 to 64
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       65 non-null     int64  
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    |   anio | indicador        |   valor |
#  |---:|-------:|:-----------------|--------:|
#  |  0 |   2021 | Definición legal | 32.7077 |
#  
#  ------------------------------
#  