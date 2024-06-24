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
drop_col(col='pais', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'serie': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    330 non-null    object 
#   1   pais    330 non-null    object 
#   2   anio    330 non-null    int64  
#   3   serie   330 non-null    object 
#   4   valor   330 non-null    float64
#  
#  |    | iso3   | pais      |   anio | serie           |    valor |
#  |---:|:-------|:----------|-------:|:----------------|---------:|
#  |  0 | ARG    | Argentina |   2021 | Serie empalmada | 0.327077 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    330 non-null    object 
#   1   anio    330 non-null    int64  
#   2   serie   330 non-null    object 
#   3   valor   330 non-null    float64
#  
#  |    | iso3   |   anio | serie           |    valor |
#  |---:|:-------|-------:|:----------------|---------:|
#  |  0 | ARG    |   2021 | Serie empalmada | 0.327077 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'serie': 'indicador'})
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  330 non-null    object 
#   1   anio       330 non-null    int64  
#   2   indicador  330 non-null    object 
#   3   valor      330 non-null    float64
#  
#  |    | geocodigo   |   anio | indicador       |    valor |
#  |---:|:------------|-------:|:----------------|---------:|
#  |  0 | ARG         |   2021 | Serie empalmada | 0.327077 |
#  
#  ------------------------------
#  