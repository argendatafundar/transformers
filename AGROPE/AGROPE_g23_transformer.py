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
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'share_expo': 'valor'}),
	drop_col(col='pais', axis=1),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	drop_na(col='valor'),
	multiplicar_por_escalar(col='valor', k=100)
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
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
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
#  drop_na(col='valor')
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
#  |  0 |   1962 |   21.81 | ARG         |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
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
#  |  0 |   1962 |   21.81 | ARG         |
#  
#  ------------------------------
#  