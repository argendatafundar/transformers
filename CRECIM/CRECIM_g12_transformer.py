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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='continente_fundar', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	drop_na(cols=['valor']),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TSA'])")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               7482 non-null   object 
#   1   pais_nombre        7482 non-null   object 
#   2   continente_fundar  5562 non-null   object 
#   3   anio               7482 non-null   int64  
#   4   cambio_relativo    7482 non-null   float64
#   5   nivel_agregacion   7482 non-null   object 
#  
#  |    | iso3   | pais_nombre                  |   continente_fundar |   anio |   cambio_relativo | nivel_agregacion   |
#  |---:|:-------|:-----------------------------|--------------------:|-------:|------------------:|:-------------------|
#  |  0 | AFE    | África Oriental y Meridional |                 nan |   1975 |                 0 | agregacion         |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              7482 non-null   object 
#   1   pais_nombre       7482 non-null   object 
#   2   anio              7482 non-null   int64  
#   3   cambio_relativo   7482 non-null   float64
#   4   nivel_agregacion  7482 non-null   object 
#  
#  |    | iso3   | pais_nombre                  |   anio |   cambio_relativo | nivel_agregacion   |
#  |---:|:-------|:-----------------------------|-------:|------------------:|:-------------------|
#  |  0 | AFE    | África Oriental y Meridional |   1975 |                 0 | agregacion         |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7482 non-null   object 
#   1   pais_nombre      7482 non-null   object 
#   2   anio             7482 non-null   int64  
#   3   cambio_relativo  7482 non-null   float64
#  
#  |    | iso3   | pais_nombre                  |   anio |   cambio_relativo |
#  |---:|:-------|:-----------------------------|-------:|------------------:|
#  |  0 | AFE    | África Oriental y Meridional |   1975 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             7482 non-null   object 
#   1   anio             7482 non-null   int64  
#   2   cambio_relativo  7482 non-null   float64
#  
#  |    | iso3   |   anio |   cambio_relativo |
#  |---:|:-------|-------:|------------------:|
#  |  0 | AFE    |   1975 |                 0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7482 non-null   object 
#   1   anio       7482 non-null   int64  
#   2   valor      7482 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   1975 |       0 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7482 non-null   object 
#   1   anio       7482 non-null   int64  
#   2   valor      7482 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   1975 |       0 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7482 non-null   object 
#   1   anio       7482 non-null   int64  
#   2   valor      7482 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   1975 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 7482 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7482 non-null   object 
#   1   anio       7482 non-null   int64  
#   2   valor      7482 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   1975 |       0 |
#  
#  ------------------------------
#  
#  query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TSA'])")
#  Index: 7146 entries, 0 to 7481
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7146 non-null   object 
#   1   anio       7146 non-null   int64  
#   2   valor      7146 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   1975 |       0 |
#  
#  ------------------------------
#  