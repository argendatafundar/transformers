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

@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	rename_columns(iso3='geocodigo', impo='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1266 entries, 0 to 1265
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    1266 non-null   int64  
#   1   iso3    1266 non-null   object 
#   2   impo    1266 non-null   float64
#  
#  |    |   anio | iso3   |   impo |
#  |---:|-------:|:-------|-------:|
#  |  0 |   2015 | NAM    |      0 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 162 entries, 8 to 1265
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    162 non-null    int64  
#   1   iso3    162 non-null    object 
#   2   impo    162 non-null    float64
#  
#  |    |   anio | iso3   |   impo |
#  |---:|-------:|:-------|-------:|
#  |  8 |   2022 | AND    |      0 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 162 entries, 8 to 1265
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    162 non-null    object 
#   1   impo    162 non-null    float64
#  
#  |    | iso3   |   impo |
#  |---:|:-------|-------:|
#  |  8 | AND    |      0 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', impo='valor')
#  Index: 162 entries, 8 to 1265
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   valor      162 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  8 | AND         |       0 |
#  
#  ------------------------------
#  