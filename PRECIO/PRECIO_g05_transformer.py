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
	rename_cols(map={'inflacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3220 entries, 0 to 3219
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   pais       3220 non-null   object 
#   1   iso3       3220 non-null   object 
#   2   anio       3220 non-null   int64  
#   3   inflacion  3152 non-null   float64
#  
#  |    | pais      | iso3   |   anio |   inflacion |
#  |---:|:----------|:-------|-------:|------------:|
#  |  0 | Argentina | ARG    |   2000 |   -0.733699 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 3220 entries, 0 to 3219
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       3220 non-null   object 
#   1   anio       3220 non-null   int64  
#   2   inflacion  3152 non-null   float64
#  
#  |    | iso3   |   anio |   inflacion |
#  |---:|:-------|-------:|------------:|
#  |  0 | ARG    |   2000 |   -0.733699 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 3220 entries, 0 to 3219
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3220 non-null   object 
#   1   anio       3220 non-null   int64  
#   2   inflacion  3152 non-null   float64
#  
#  |    | geocodigo   |   anio |   inflacion |
#  |---:|:------------|-------:|------------:|
#  |  0 | ARG         |   2000 |   -0.733699 |
#  
#  ------------------------------
#  
#  rename_cols(map={'inflacion': 'valor'})
#  RangeIndex: 3220 entries, 0 to 3219
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3220 non-null   object 
#   1   anio       3220 non-null   int64  
#   2   valor      3152 non-null   float64
#  
#  |    | geocodigo   |   anio |     valor |
#  |---:|:------------|-------:|----------:|
#  |  0 | ARG         |   2000 | -0.733699 |
#  
#  ------------------------------
#  