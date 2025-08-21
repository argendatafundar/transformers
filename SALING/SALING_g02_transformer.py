from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  14 non-null     object 
#   1   geonombreFundar  14 non-null     object 
#   2   year             14 non-null     int64  
#   3   ipcf_promedio    14 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year |   ipcf_promedio |
#  |---:|:------------------|:------------------|-------:|----------------:|
#  |  0 | ARG               | Argentina         |   2022 |         695.306 |
#  
#  ------------------------------
#  