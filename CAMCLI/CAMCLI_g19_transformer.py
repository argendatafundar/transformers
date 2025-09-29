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

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'year': 'anio', 'near_surface_temperature_anomaly': 'valor'}),
	pd_loc(expr='df["anio"] < df["anio"].max()')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 4 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   geocodigoFundar                   176 non-null    object 
#   1   year                              176 non-null    int64  
#   2   near_surface_temperature_anomaly  176 non-null    float64
#   3   geonombreFundar                   176 non-null    object 
#  
#  |    | geocodigoFundar   |   year |   near_surface_temperature_anomaly | geonombreFundar   |
#  |---:|:------------------|-------:|-----------------------------------:|:------------------|
#  |  0 | WLD               |   1850 |                         -0.0554137 | Mundo             |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'near_surface_temperature_anomaly': 'valor'})
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  176 non-null    object 
#   1   anio             176 non-null    int64  
#   2   valor            176 non-null    float64
#   3   geonombreFundar  176 non-null    object 
#  
#  |    | geocodigoFundar   |   anio |      valor | geonombreFundar   |
#  |---:|:------------------|-------:|-----------:|:------------------|
#  |  0 | WLD               |   1850 | -0.0554137 | Mundo             |
#  
#  ------------------------------
#  
#  pd_loc(expr='df["anio"] < df["anio"].max()')
#  Index: 175 entries, 0 to 174
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  175 non-null    object 
#   1   anio             175 non-null    int64  
#   2   valor            175 non-null    float64
#   3   geonombreFundar  175 non-null    object 
#  
#  |    | geocodigoFundar   |   anio |      valor | geonombreFundar   |
#  |---:|:------------------|-------:|-----------:|:------------------|
#  |  0 | WLD               |   1850 | -0.0554137 | Mundo             |
#  
#  ------------------------------
#  