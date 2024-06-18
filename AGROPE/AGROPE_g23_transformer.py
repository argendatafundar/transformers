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
rename_cols(map={'iso3': 'geocodigo', 'share_expo': 'valor'}),
	drop_col(col='pais', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        240 non-null    int64  
#   1   share_expo  240 non-null    float64
#   2   iso3        240 non-null    object 
#   3   pais        240 non-null    object 
#  
#  |    |   anio |   share_expo | iso3   | pais      |
#  |---:|-------:|-------------:|:-------|:----------|
#  |  0 |   1962 |       0.2181 | ARG    | Argentina |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'share_expo': 'valor'})
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       240 non-null    int64  
#   1   valor      240 non-null    float64
#   2   geocodigo  240 non-null    object 
#   3   pais       240 non-null    object 
#  
#  |    |   anio |   valor | geocodigo   | pais      |
#  |---:|-------:|--------:|:------------|:----------|
#  |  0 |   1962 |  0.2181 | ARG         | Argentina |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       240 non-null    int64  
#   1   valor      240 non-null    float64
#   2   geocodigo  240 non-null    object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   1962 |  0.2181 | ARG         |
#  
#  ------------------------------
#  