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
	query(condition='anio.isin([2007, 2015, 2022])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  80 non-null     object 
#   1   geonombreFundar  80 non-null     object 
#   2   anio             80 non-null     int64  
#   3   valor            80 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([2007, 2015, 2022])')
#  Index: 15 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  15 non-null     object 
#   1   geonombreFundar  15 non-null     object 
#   2   anio             15 non-null     int64  
#   3   valor            15 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  