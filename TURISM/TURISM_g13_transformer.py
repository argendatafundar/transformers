from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 72 entries, 0 to 71
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         72 non-null     int64  
#   1   rama_etq     72 non-null     object 
#   2   fem_tasa     72 non-null     float64
#   3   joven_tasa   72 non-null     float64
#   4   inform_tasa  72 non-null     float64
#  
#  |    |   anio | rama_etq          |   fem_tasa |   joven_tasa |   inform_tasa |
#  |---:|-------:|:------------------|-----------:|-------------:|--------------:|
#  |  0 |   2016 | Agencias de viaje |     44.483 |      28.7972 |       26.5607 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 8 entries, 48 to 71
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         8 non-null      int64  
#   1   rama_etq     8 non-null      object 
#   2   fem_tasa     8 non-null      float64
#   3   joven_tasa   8 non-null      float64
#   4   inform_tasa  8 non-null      float64
#  
#  |    |   anio | rama_etq          |   fem_tasa |   joven_tasa |   inform_tasa |
#  |---:|-------:|:------------------|-----------:|-------------:|--------------:|
#  | 48 |   2024 | Agencias de viaje |    57.9139 |      48.7608 |        21.449 |
#  
#  ------------------------------
#  