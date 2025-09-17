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
	query(condition='anio <= 2025')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36024 entries, 0 to 36023
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   36024 non-null  int64  
#   1   geocodigoFundar        36024 non-null  object 
#   2   geonombreFundar        36024 non-null  object 
#   3   edad_media_maternidad  35787 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   edad_media_maternidad |
#  |---:|-------:|:------------------|:------------------|------------------------:|
#  |  0 |   1950 | BDI               | Burundi           |                  30.674 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 18012 entries, 0 to 35947
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   18012 non-null  int64  
#   1   geocodigoFundar        18012 non-null  object 
#   2   geonombreFundar        18012 non-null  object 
#   3   edad_media_maternidad  18012 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   edad_media_maternidad |
#  |---:|-------:|:------------------|:------------------|------------------------:|
#  |  0 |   1950 | BDI               | Burundi           |                  30.674 |
#  
#  ------------------------------
#  