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

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='provincia_nombre', axis=1),
	rename_cols(map={'provincia_id': 'geocodigo'}),
	rename_cols(map={'pbg_pc_relativo': 'valor'}),
	latest_year(by='anio'),
	sort_values(how='descending', by='valor'),
	multiplicar_por_escalar(col='valor', k=0.01)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      432 non-null    object 
#   1   provincia_nombre  432 non-null    object 
#   2   anio              432 non-null    int64  
#   3   pbg_pc_relativo   432 non-null    float64
#  
#  |    | provincia_id   | provincia_nombre                |   anio |   pbg_pc_relativo |
#  |---:|:---------------|:--------------------------------|-------:|------------------:|
#  |  0 | AR-C           | Ciudad Autónoma de Buenos Aires |   2004 |             274.4 |
#  
#  ------------------------------
#  
#  drop_col(col='provincia_nombre', axis=1)
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   provincia_id     432 non-null    object 
#   1   anio             432 non-null    int64  
#   2   pbg_pc_relativo  432 non-null    float64
#  
#  |    | provincia_id   |   anio |   pbg_pc_relativo |
#  |---:|:---------------|-------:|------------------:|
#  |  0 | AR-C           |   2004 |             274.4 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo'})
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        432 non-null    object 
#   1   anio             432 non-null    int64  
#   2   pbg_pc_relativo  432 non-null    float64
#  
#  |    | geocodigo   |   anio |   pbg_pc_relativo |
#  |---:|:------------|-------:|------------------:|
#  |  0 | AR-C        |   2004 |             274.4 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pbg_pc_relativo': 'valor'})
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  432 non-null    object 
#   1   anio       432 non-null    int64  
#   2   valor      432 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   2004 |   274.4 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 24 entries, 17 to 431
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  24 non-null     object 
#   1   valor      24 non-null     float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 17 | AR-C        |   293.3 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='valor')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  24 non-null     object 
#   1   valor      24 non-null     float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AR-C        |   2.933 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=0.01)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  24 non-null     object 
#   1   valor      24 non-null     float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AR-C        |   2.933 |
#  
#  ------------------------------
#  