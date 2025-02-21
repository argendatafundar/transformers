from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'relativo_arg': 'valor'}),
	query(condition='anio > 1819')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17047 entries, 0 to 17046
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          17047 non-null  int64  
#   1   iso3          17047 non-null  object 
#   2   relativo_arg  15936 non-null  float64
#  
#  |    |   anio | iso3   |   relativo_arg |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1950 | AFG    |         6.8763 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'relativo_arg': 'valor'})
#  RangeIndex: 17047 entries, 0 to 17046
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       17047 non-null  int64  
#   1   geocodigo  17047 non-null  object 
#   2   valor      15936 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1950 | AFG         |  6.8763 |
#  
#  ------------------------------
#  
#  query(condition='anio > 1819')
#  Index: 17047 entries, 0 to 17046
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       17047 non-null  int64  
#   1   geocodigo  17047 non-null  object 
#   2   valor      15936 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1950 | AFG         |  6.8763 |
#  
#  ------------------------------
#  