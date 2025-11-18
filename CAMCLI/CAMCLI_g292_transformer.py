from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df, dummy = True):
    import polars as pl
    if isinstance(df, pl.DataFrame):
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
#  RangeIndex: 212 entries, 0 to 211
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              195 non-null    object 
#   1   temperature_anomaly_40_69    212 non-null    float64
#   2   temperature_anomaly_20_adel  212 non-null    float64
#   3   dif                          212 non-null    float64
#   4   geonombreFundar              212 non-null    object 
#  
#  |    | geocodigoFundar   |   temperature_anomaly_40_69 |   temperature_anomaly_20_adel |     dif | geonombreFundar   |
#  |---:|:------------------|----------------------------:|------------------------------:|--------:|:------------------|
#  |  0 | NOR               |                    -2.41211 |                      0.674379 | 3.08649 | Noruega           |
#  
#  ------------------------------
#  