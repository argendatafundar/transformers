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
rename_cols(map={'region': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    80 non-null     int64  
#   1   region  80 non-null     object 
#   2   valor   80 non-null     float64
#  
#  |    |   anio | region   |       valor |
#  |---:|-------:|:---------|------------:|
#  |  0 |   2007 | Cuyo     | 2.01333e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'indicador'})
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       80 non-null     int64  
#   1   indicador  80 non-null     object 
#   2   valor      80 non-null     float64
#  
#  |    |   anio | indicador   |       valor |
#  |---:|-------:|:------------|------------:|
#  |  0 |   2007 | Cuyo        | 2.01333e+06 |
#  
#  ------------------------------
#  