from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(col='valor')
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
#  drop_na(col='valor')
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