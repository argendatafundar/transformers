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
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

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
	drop_na(subset='valor'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        6747 non-null   int64  
#   1   share_expo  6747 non-null   float64
#   2   iso3        6747 non-null   object 
#   3   pais        6747 non-null   object 
#  
#  |    |   anio |   share_expo | iso3   | pais   |
#  |---:|-------:|-------------:|:-------|:-------|
#  |  0 |   2014 |            0 | ABW    | Aruba  |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'share_expo': 'valor'})
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6747 non-null   int64  
#   1   valor      6747 non-null   float64
#   2   geocodigo  6747 non-null   object 
#   3   pais       6747 non-null   object 
#  
#  |    |   anio |   valor | geocodigo   | pais   |
#  |---:|-------:|--------:|:------------|:-------|
#  |  0 |   2014 |       0 | ABW         | Aruba  |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6747 non-null   int64  
#   1   valor      6747 non-null   float64
#   2   geocodigo  6747 non-null   object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   2014 |       0 | ABW         |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6747 non-null   int64  
#   1   valor      6747 non-null   float64
#   2   geocodigo  6747 non-null   object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   1962 |  0.0009 | AGO         |
#  
#  ------------------------------
#  
#  drop_na(subset='valor')
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6747 non-null   int64  
#   1   valor      6747 non-null   float64
#   2   geocodigo  6747 non-null   object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   1962 |    0.09 | AGO         |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       6747 non-null   int64  
#   1   valor      6747 non-null   float64
#   2   geocodigo  6747 non-null   object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   1962 |    0.09 | AGO         |
#  
#  ------------------------------
#  