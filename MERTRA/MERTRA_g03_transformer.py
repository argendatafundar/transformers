from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
drop_col(col='iso3_desc', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'tasa_actividad': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16758 entries, 0 to 16757
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              16758 non-null  object 
#   1   iso3_desc         16758 non-null  object 
#   2   anio              16758 non-null  int64  
#   3   tasa_actividad    7749 non-null   float64
#   4   nivel_agregacion  16695 non-null  object 
#  
#  |    | iso3   | iso3_desc   |   anio |   tasa_actividad | nivel_agregacion   |
#  |---:|:-------|:------------|-------:|-----------------:|:-------------------|
#  |  0 | ABW    | Aruba       |   1960 |              nan | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc', axis=1)
#  RangeIndex: 16758 entries, 0 to 16757
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              16758 non-null  object 
#   1   anio              16758 non-null  int64  
#   2   tasa_actividad    7749 non-null   float64
#   3   nivel_agregacion  16695 non-null  object 
#  
#  |    | iso3   |   anio |   tasa_actividad | nivel_agregacion   |
#  |---:|:-------|-------:|-----------------:|:-------------------|
#  |  0 | ABW    |   1960 |              nan | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 16758 entries, 0 to 16757
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            16758 non-null  object 
#   1   anio            16758 non-null  int64  
#   2   tasa_actividad  7749 non-null   float64
#  
#  |    | iso3   |   anio |   tasa_actividad |
#  |---:|:-------|-------:|-----------------:|
#  |  0 | ABW    |   1960 |              nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'tasa_actividad': 'valor'})
#  RangeIndex: 16758 entries, 0 to 16757
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16758 non-null  object 
#   1   anio       16758 non-null  int64  
#   2   valor      7749 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1960 |     nan |
#  
#  ------------------------------
#  