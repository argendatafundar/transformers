from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
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
	drop_na(col='geocodigoFundar'),
	rename_cols(map={'geonombreFundar': 'name_long'})
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
#  drop_na(col='geocodigoFundar')
#  Index: 195 entries, 0 to 212
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              195 non-null    object 
#   1   temperature_anomaly_40_69    195 non-null    float64
#   2   temperature_anomaly_20_adel  195 non-null    float64
#   3   dif                          195 non-null    float64
#   4   geonombreFundar              195 non-null    object 
#  
#  |    | geocodigoFundar   |   temperature_anomaly_40_69 |   temperature_anomaly_20_adel |     dif | geonombreFundar   |
#  |---:|:------------------|----------------------------:|------------------------------:|--------:|:------------------|
#  |  0 | NOR               |                    -2.41211 |                      0.674379 | 3.08649 | Noruega           |
#  
#  ------------------------------
#  
#  rename_cols(map={'geonombreFundar': 'name_long'})
#  Index: 195 entries, 0 to 212
#  Data columns (total 5 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              195 non-null    object 
#   1   temperature_anomaly_40_69    195 non-null    float64
#   2   temperature_anomaly_20_adel  195 non-null    float64
#   3   dif                          195 non-null    float64
#   4   name_long                    195 non-null    object 
#  
#  |    | geocodigoFundar   |   temperature_anomaly_40_69 |   temperature_anomaly_20_adel |     dif | name_long   |
#  |---:|:------------------|----------------------------:|------------------------------:|--------:|:------------|
#  |  0 | NOR               |                    -2.41211 |                      0.674379 | 3.08649 | Noruega     |
#  
#  ------------------------------
#  