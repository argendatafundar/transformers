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
	rename_cols(map={'salario_real_base1970': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   89 non-null     int64  
#   1   salario_real_base1970  89 non-null     float64
#  
#  |    |   anio |   salario_real_base1970 |
#  |---:|-------:|------------------------:|
#  |  0 |   1935 |                 50.9449 |
#  
#  ------------------------------
#  
#  rename_cols(map={'salario_real_base1970': 'valor'})
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    89 non-null     int64  
#   1   valor   89 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1935 | 50.9449 |
#  
#  ------------------------------
#  