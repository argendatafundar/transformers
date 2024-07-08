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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='continente_fundar', axis=1),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	drop_col(col='pib_per_capita', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD'),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               7831 non-null   object 
#   1   pais_nombre        7831 non-null   object 
#   2   continente_fundar  7611 non-null   object 
#   3   es_agregacion      7831 non-null   int64  
#   4   anio               7831 non-null   int64  
#   5   pib_per_capita     7831 non-null   float64
#   6   cambio_relativo    7831 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|-----------------:|------------------:|
#  |  0 | ARG    | Argentina     | América del Sur     |               0 |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7831 non-null   object 
#   1   pais_nombre      7831 non-null   object 
#   2   es_agregacion    7831 non-null   int64  
#   3   anio             7831 non-null   int64  
#   4   pib_per_capita   7831 non-null   float64
#   5   cambio_relativo  7831 non-null   float64
#  
#  |    | iso3   | pais_nombre   |   es_agregacion |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:-------|:--------------|----------------:|-------:|-----------------:|------------------:|
#  |  0 | ARG    | Argentina     |               0 |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7831 non-null   object 
#   1   pais_nombre      7831 non-null   object 
#   2   anio             7831 non-null   int64  
#   3   pib_per_capita   7831 non-null   float64
#   4   cambio_relativo  7831 non-null   float64
#  
#  |    | iso3   | pais_nombre   |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:-------|:--------------|-------:|-----------------:|------------------:|
#  |  0 | ARG    | Argentina     |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7831 non-null   object 
#   1   anio             7831 non-null   int64  
#   2   pib_per_capita   7831 non-null   float64
#   3   cambio_relativo  7831 non-null   float64
#  
#  |    | iso3   |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:-------|-------:|-----------------:|------------------:|
#  |  0 | ARG    |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pib_per_capita', axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7831 non-null   object 
#   1   anio             7831 non-null   int64  
#   2   cambio_relativo  7831 non-null   float64
#  
#  |    | iso3   |   anio |   cambio_relativo |
#  |---:|:-------|-------:|------------------:|
#  |  0 | ARG    |   1820 |                 0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7831 non-null   object 
#   1   anio       7831 non-null   int64  
#   2   valor      7831 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   1820 |       0 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7831 non-null   object 
#   1   anio       7831 non-null   int64  
#   2   valor      7831 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   1820 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD')
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7831 non-null   object 
#   1   anio       7831 non-null   int64  
#   2   valor      7831 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   1820 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7831 non-null   object 
#   1   anio       7831 non-null   int64  
#   2   valor      7831 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   1820 |       0 |
#  
#  ------------------------------
#  