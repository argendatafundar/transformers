from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def compute_col(df, new_col, expr):
    df[new_col] = eval(expr)
    return df

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
	compute_col(new_col='categoria', expr="df['temperature_anomaly'] > 0")
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 5 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      85 non-null     object 
#   1   geocodigoFundar      85 non-null     object 
#   2   year                 85 non-null     int64  
#   3   temperature_anomaly  85 non-null     float64
#   4   categoria            85 non-null     bool   
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly | categoria   |
#  |---:|:------------------|:------------------|-------:|----------------------:|:------------|
#  |  0 | Argentina         | ARG               |   1940 |             -0.520686 | False       |
#  
#  ------------------------------
#  
#  compute_col(new_col='categoria', expr="df['temperature_anomaly'] > 0")
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 5 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      85 non-null     object 
#   1   geocodigoFundar      85 non-null     object 
#   2   year                 85 non-null     int64  
#   3   temperature_anomaly  85 non-null     float64
#   4   categoria            85 non-null     bool   
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly | categoria   |
#  |---:|:------------------|:------------------|-------:|----------------------:|:------------|
#  |  0 | Argentina         | ARG               |   1940 |             -0.520686 | False       |
#  
#  ------------------------------
#  