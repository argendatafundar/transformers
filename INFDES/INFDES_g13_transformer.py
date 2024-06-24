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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais_desc', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'estado': 'indicador', 'satisfaccion_vida': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 189 entries, 0 to 188
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               189 non-null    object 
#   1   pais_desc          189 non-null    object 
#   2   anio               189 non-null    int64  
#   3   estado             189 non-null    object 
#   4   satisfaccion_vida  189 non-null    float64
#  
#  |    | iso3   | pais_desc   |   anio | estado   |   satisfaccion_vida |
#  |---:|:-------|:------------|-------:|:---------|--------------------:|
#  |  0 | AND    | Andorra     |   2018 | Ocupado  |             7.59255 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  RangeIndex: 189 entries, 0 to 188
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               189 non-null    object 
#   1   anio               189 non-null    int64  
#   2   estado             189 non-null    object 
#   3   satisfaccion_vida  189 non-null    float64
#  
#  |    | iso3   |   anio | estado   |   satisfaccion_vida |
#  |---:|:-------|-------:|:---------|--------------------:|
#  |  0 | AND    |   2018 | Ocupado  |             7.59255 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'estado': 'indicador', 'satisfaccion_vida': 'valor'})
#  RangeIndex: 189 entries, 0 to 188
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  189 non-null    object 
#   1   anio       189 non-null    int64  
#   2   indicador  189 non-null    object 
#   3   valor      189 non-null    float64
#  
#  |    | geocodigo   |   anio | indicador   |   valor |
#  |---:|:------------|-------:|:------------|--------:|
#  |  0 | AND         |   2018 | Ocupado     | 7.59255 |
#  
#  ------------------------------
#  