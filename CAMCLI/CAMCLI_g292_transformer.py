from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
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
	replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Arctic Ocean': 'Océano Ártico', 'Oceania': 'Oceanía', 'Southern Ocean': 'Océano Antártico', 'South Atlantic Ocean': 'Océano Atlántico Sur', 'North Atlantic Ocean': 'Océano Atlántico Norte', 'North Pacific Ocean': 'Océano Pacífico Norte', 'Indian Ocean (NIAID': 'Océano Índico', 'South Pacific Ocean': 'Océano Pacífico Sur', 'North America': 'América del Norte', 'South America': 'América del Sur', 'Europe': 'Europa'}})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 213 entries, 0 to 212
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              195 non-null    object 
#   1   temperature_anomaly_40_69    213 non-null    float64
#   2   temperature_anomaly_20_adel  213 non-null    float64
#   3   dif                          213 non-null    float64
#   4   geonombreFundar              213 non-null    object 
#  
#  |    | geocodigoFundar   |   temperature_anomaly_40_69 |   temperature_anomaly_20_adel |     dif | geonombreFundar   |
#  |---:|:------------------|----------------------------:|------------------------------:|--------:|:------------------|
#  |  0 | NOR               |                    -2.41211 |                      0.674379 | 3.08649 | Noruega           |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Arctic Ocean': 'Océano Ártico', 'Oceania': 'Oceanía', 'Southern Ocean': 'Océano Antártico', 'South Atlantic Ocean': 'Océano Atlántico Sur', 'North Atlantic Ocean': 'Océano Atlántico Norte', 'North Pacific Ocean': 'Océano Pacífico Norte', 'Indian Ocean (NIAID': 'Océano Índico', 'South Pacific Ocean': 'Océano Pacífico Sur', 'North America': 'América del Norte', 'South America': 'América del Sur', 'Europe': 'Europa'}})
#  RangeIndex: 213 entries, 0 to 212
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              195 non-null    object 
#   1   temperature_anomaly_40_69    213 non-null    float64
#   2   temperature_anomaly_20_adel  213 non-null    float64
#   3   dif                          213 non-null    float64
#   4   geonombreFundar              213 non-null    object 
#  
#  |    | geocodigoFundar   |   temperature_anomaly_40_69 |   temperature_anomaly_20_adel |     dif | geonombreFundar   |
#  |---:|:------------------|----------------------------:|------------------------------:|--------:|:------------------|
#  |  0 | NOR               |                    -2.41211 |                      0.674379 | 3.08649 | Noruega           |
#  
#  ------------------------------
#  