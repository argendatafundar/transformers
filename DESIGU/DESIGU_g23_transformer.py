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
rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       90 non-null     int64  
#   1   variable  90 non-null     object 
#   2   valor     90 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |  66.123 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       90 non-null     int64  
#   1   indicador  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | quintil1    |  66.123 |
#  
#  ------------------------------
#  