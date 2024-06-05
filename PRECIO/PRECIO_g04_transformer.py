from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais', axis=1),
	rename_cols(map={'iso3': 'geocodigo'}),
	rename_cols(map={'inflacion_prom_07_22': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   pais                  140 non-null    object 
#   1   iso3                  140 non-null    object 
#   2   inflacion_prom_07_22  140 non-null    float64
#  
#  |    | pais       | iso3   |   inflacion_prom_07_22 |
#  |---:|:-----------|:-------|-----------------------:|
#  |  0 | Afganistán | AFG    |                5.46471 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 2 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  140 non-null    object 
#   1   inflacion_prom_07_22  140 non-null    float64
#  
#  |    | iso3   |   inflacion_prom_07_22 |
#  |---:|:-------|-----------------------:|
#  |  0 | AFG    |                5.46471 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 2 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigo             140 non-null    object 
#   1   inflacion_prom_07_22  140 non-null    float64
#  
#  |    | geocodigo   |   inflacion_prom_07_22 |
#  |---:|:------------|-----------------------:|
#  |  0 | AFG         |                5.46471 |
#  
#  ------------------------------
#  
#  rename_cols(map={'inflacion_prom_07_22': 'valor'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  140 non-null    object 
#   1   valor      140 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AFG         | 5.46471 |
#  
#  ------------------------------
#  