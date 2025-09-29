from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pd_loc(df: DataFrame, expr: str):
    import pandas as pd
    import numpy as np

    return df.loc[eval(expr)]

@transformer.convert
def to_pandas(df, dummy = True):
    import polars as pl
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	pd_loc(expr="df['year'] % 2 == 0")
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 4 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      85 non-null     object 
#   1   geocodigoFundar      85 non-null     object 
#   2   year                 85 non-null     int64  
#   3   temperature_anomaly  85 non-null     float64
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly |
#  |---:|:------------------|:------------------|-------:|----------------------:|
#  |  0 | Argentina         | ARG               |   1940 |             -0.520686 |
#  
#  ------------------------------
#  
#  pd_loc(expr="df['year'] % 2 == 0")
#  Index: 43 entries, 0 to 84
#  Data columns (total 4 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      43 non-null     object 
#   1   geocodigoFundar      43 non-null     object 
#   2   year                 43 non-null     int64  
#   3   temperature_anomaly  43 non-null     float64
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly |
#  |---:|:------------------|:------------------|-------:|----------------------:|
#  |  0 | Argentina         | ARG               |   1940 |             -0.520686 |
#  
#  ------------------------------
#  