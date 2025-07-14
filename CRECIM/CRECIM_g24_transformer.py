from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'region_pbg'], axis=1),
	query(condition='anio >= 2004')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  672 non-null    object 
#   1   geonombreFundar  672 non-null    object 
#   2   region_pbg       672 non-null    object 
#   3   anio             672 non-null    int64  
#   4   pib_pc           672 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | region_pbg      |   anio |   pib_pc |
#  |---:|:------------------|:------------------|:----------------|-------:|---------:|
#  |  0 | AR-B              | Buenos Aires      | Pampeana y AMBA |   1895 |  4413.34 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'region_pbg'], axis=1)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  672 non-null    object 
#   1   anio             672 non-null    int64  
#   2   pib_pc           672 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   pib_pc |
#  |---:|:------------------|-------:|---------:|
#  |  0 | Buenos Aires      |   1895 |  4413.34 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 2004')
#  Index: 456 entries, 9 to 671
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  456 non-null    object 
#   1   anio             456 non-null    int64  
#   2   pib_pc           456 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   pib_pc |
#  |---:|:------------------|-------:|---------:|
#  |  9 | Buenos Aires      |   2004 |  8989.67 |
#  
#  ------------------------------
#  