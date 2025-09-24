from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'brecha': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   ano     23 non-null     int64  
#   1   brecha  23 non-null     float64
#  
#  |    |   ano |   brecha |
#  |---:|------:|---------:|
#  |  0 |  2000 |    0.138 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'brecha': 'valor'})
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    23 non-null     int64  
#   1   valor   23 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2000 |    13.8 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    23 non-null     int64  
#   1   valor   23 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2000 |    13.8 |
#  
#  ------------------------------
#  