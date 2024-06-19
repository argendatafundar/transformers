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

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='location_name_short_en', axis=1),
	rename_cols(map={'x_tt_pc': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    12912 non-null  int64  
#   1   iso3                    12912 non-null  object 
#   2   location_name_short_en  12912 non-null  object 
#   3   x_tt_pc                 12912 non-null  float64
#  
#  |    |   anio | iso3   | location_name_short_en   |   x_tt_pc |
#  |---:|-------:|:-------|:-------------------------|----------:|
#  |  0 |   1962 | AFG    | Afghanistan              | 0.0649777 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    12912 non-null  int64  
#   1   geocodigo               12912 non-null  object 
#   2   location_name_short_en  12912 non-null  object 
#   3   x_tt_pc                 12912 non-null  float64
#  
#  |    |   anio | geocodigo   | location_name_short_en   |   x_tt_pc |
#  |---:|-------:|:------------|:-------------------------|----------:|
#  |  0 |   1962 | AFG         | Afghanistan              | 0.0649777 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12912 non-null  int64  
#   1   geocodigo  12912 non-null  object 
#   2   x_tt_pc    12912 non-null  float64
#  
#  |    |   anio | geocodigo   |   x_tt_pc |
#  |---:|-------:|:------------|----------:|
#  |  0 |   1962 | AFG         | 0.0649777 |
#  
#  ------------------------------
#  
#  rename_cols(map={'x_tt_pc': 'valor'})
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12912 non-null  int64  
#   1   geocodigo  12912 non-null  object 
#   2   valor      12912 non-null  float64
#  
#  |    |   anio | geocodigo   |     valor |
#  |---:|-------:|:------------|----------:|
#  |  0 |   1962 | AFG         | 0.0649777 |
#  
#  ------------------------------
#  