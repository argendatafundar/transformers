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
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  60 non-null     object 
#   1   geonombreFundar  60 non-null     object 
#   2   year             60 non-null     int64  
#   3   ipcf_promedio    60 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year |   ipcf_promedio |
#  |---:|:------------------|:------------------|-------:|----------------:|
#  |  0 | ARG               | Argentina         |   1992 |         699.336 |
#  
#  ------------------------------
#  