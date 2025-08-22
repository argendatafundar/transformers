from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'indice': 'valor'}),
	query(condition='anio in [anio.min(), anio.max(), 1984]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 45 entries, 0 to 44
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   ano     45 non-null     int64  
#   1   indice  45 non-null     float64
#  
#  |    |   ano |   indice |
#  |---:|------:|---------:|
#  |  0 |  1980 | 0.628549 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'indice': 'valor'})
#  RangeIndex: 45 entries, 0 to 44
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    45 non-null     int64  
#   1   valor   45 non-null     float64
#  
#  |    |   anio |    valor |
#  |---:|-------:|---------:|
#  |  0 |   1980 | 0.628549 |
#  
#  ------------------------------
#  
#  query(condition='anio in [anio.min(), anio.max(), 1984]')
#  Index: 3 entries, 0 to 44
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    3 non-null      int64  
#   1   valor   3 non-null      float64
#  
#  |    |   anio |    valor |
#  |---:|-------:|---------:|
#  |  0 |   1980 | 0.628549 |
#  
#  ------------------------------
#  