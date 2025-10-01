from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pd_loc(df, expr):
    return df.loc[eval(expr)]
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pd_loc(expr="df['year'].isin([2024,\n 2019,\n 2014,\n 2009,\n 2004,\n 1999,\n 1994,\n 1989,\n 1984,\n 1979,\n 1974,\n 1969,\n 1964,\n 1959,\n 1954,\n 1949,\n 1944])")
)
#  PIPELINE_END


#  start()
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
#  |  0 | Argentina         | ARG               |   1940 |              -0.94151 |
#  
#  ------------------------------
#  
#  pd_loc(expr="df['year'].isin([2024,\n 2019,\n 2014,\n 2009,\n 2004,\n 1999,\n 1994,\n 1989,\n 1984,\n 1979,\n 1974,\n 1969,\n 1964,\n 1959,\n 1954,\n 1949,\n 1944])")
#  Index: 17 entries, 4 to 84
#  Data columns (total 4 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   geonombreFundar      17 non-null     object 
#   1   geocodigoFundar      17 non-null     object 
#   2   year                 17 non-null     int64  
#   3   temperature_anomaly  17 non-null     float64
#  
#  |    | geonombreFundar   | geocodigoFundar   |   year |   temperature_anomaly |
#  |---:|:------------------|:------------------|-------:|----------------------:|
#  |  4 | Argentina         | ARG               |   1944 |              0.202888 |
#  
#  ------------------------------
#  