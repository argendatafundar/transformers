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
drop_col(col='pais', axis=1),
	drop_col(col='serie', axis=1),
	rename_cols(map={'iso3': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    286 non-null    object 
#   1   pais    286 non-null    object 
#   2   anio    286 non-null    int64  
#   3   valor   286 non-null    float64
#   4   serie   286 non-null    object 
#  
#  |    | iso3   | pais      |   anio |    valor | serie           |
#  |---:|:-------|:----------|-------:|---------:|:----------------|
#  |  0 | ARG    | Argentina |   1986 | 0.268756 | Serie empalmada |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    286 non-null    object 
#   1   anio    286 non-null    int64  
#   2   valor   286 non-null    float64
#   3   serie   286 non-null    object 
#  
#  |    | iso3   |   anio |    valor | serie           |
#  |---:|:-------|-------:|---------:|:----------------|
#  |  0 | ARG    |   1986 | 0.268756 | Serie empalmada |
#  
#  ------------------------------
#  
#  drop_col(col='serie', axis=1)
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    286 non-null    object 
#   1   anio    286 non-null    int64  
#   2   valor   286 non-null    float64
#  
#  |    | iso3   |   anio |    valor |
#  |---:|:-------|-------:|---------:|
#  |  0 | ARG    |   1986 | 0.268756 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 286 entries, 0 to 285
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  286 non-null    object 
#   1   anio       286 non-null    int64  
#   2   valor      286 non-null    float64
#  
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | ARG         |   1986 | 0.268756 |
#  
#  ------------------------------
#  