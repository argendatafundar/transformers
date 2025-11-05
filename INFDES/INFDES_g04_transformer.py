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
	query(condition='anio>1986')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 67 entries, 0 to 66
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               67 non-null     int64  
#   1   tipo_informalidad  67 non-null     object 
#   2   valor              67 non-null     float64
#  
#  |    |   anio | tipo_informalidad     |   valor |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1988 | Definición productiva | 48.1013 |
#  
#  ------------------------------
#  
#  query(condition='anio>1986')
#  Index: 66 entries, 0 to 66
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               66 non-null     int64  
#   1   tipo_informalidad  66 non-null     object 
#   2   valor              66 non-null     float64
#  
#  |    |   anio | tipo_informalidad     |   valor |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   1988 | Definición productiva | 48.1013 |
#  
#  ------------------------------
#  