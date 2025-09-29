from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df, dummy = True):
    import polars as pl
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    return df

@transformer.convert
def pd_loc(df, expr):
    return df.loc[eval(expr)]
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	pd_loc(expr="df['year'] % 4 == 0")
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
#  pd_loc(expr="df['year'] % 4 == 0")
#  Index: 22 entries, 0 to 84
#  Data columns (total 4 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      22 non-null     object 
#   1   geocodigoFundar      22 non-null     object 
#   2   year                 22 non-null     int64  
#   3   temperature_anomaly  22 non-null     float64
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly |
#  |---:|:------------------|:------------------|-------:|----------------------:|
#  |  0 | Argentina         | ARG               |   1940 |             -0.520686 |
#  
#  ------------------------------
#  