from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='iso3_desc_fundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 195 entries, 0 to 194
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              195 non-null    object 
#   1   iso3_desc_fundar  195 non-null    object 
#   2   valor             195 non-null    float64
#  
#  |    | iso3   | iso3_desc_fundar   |   valor |
#  |---:|:-------|:-------------------|--------:|
#  |  0 | AFG    | Afganistán         |   28480 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 195 entries, 0 to 194
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         195 non-null    object 
#   1   iso3_desc_fundar  195 non-null    object 
#   2   valor             195 non-null    float64
#  
#  |    | geocodigo   | iso3_desc_fundar   |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AFG         | Afganistán         |   28480 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 195 entries, 0 to 194
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  195 non-null    object 
#   1   valor      195 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AFG         |   28480 |
#  
#  ------------------------------
#  