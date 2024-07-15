from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2021 & pais.isin(["Bolivia", "Paraguay", "Ecuador", "Países de ingreso medio", "Uruguay", "Colombia", "Brasil", "Argentina", "Mundo", "Chile", "Países de ingreso alto"])'),
	rename_cols(map={'pais': 'categoria', 'va_agro_sobre_pbi': 'valor'}),
	sort_values(how='descending', by='valor'),
	drop_col(col=['iso3c', 'anio'], axis=1),
	replace_value(col='categoria', curr_value='Mundo', new_value='Media mundial')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              756 non-null    object 
#   1   anio               756 non-null    int64  
#   2   va_agro_sobre_pbi  631 non-null    float64
#   3   pais               756 non-null    object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais                   |
#  |---:|:--------|-------:|--------------------:|:-----------------------|
#  |  0 | HIC     |   2022 |                 nan | Países de ingreso alto |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021 & pais.isin(["Bolivia", "Paraguay", "Ecuador", "Países de ingreso medio", "Uruguay", "Colombia", "Brasil", "Argentina", "Mundo", "Chile", "Países de ingreso alto"])')
#  Index: 11 entries, 1 to 694
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              11 non-null     object 
#   1   anio               11 non-null     int64  
#   2   va_agro_sobre_pbi  11 non-null     float64
#   3   pais               11 non-null     object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais                   |
#  |---:|:--------|-------:|--------------------:|:-----------------------|
#  |  1 | HIC     |   2021 |             1.27667 | Países de ingreso alto |
#  
#  ------------------------------
#  
#  rename_cols(map={'pais': 'categoria', 'va_agro_sobre_pbi': 'valor'})
#  Index: 11 entries, 1 to 694
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3c      11 non-null     object 
#   1   anio       11 non-null     int64  
#   2   valor      11 non-null     float64
#   3   categoria  11 non-null     object 
#  
#  |    | iso3c   |   anio |   valor | categoria              |
#  |---:|:--------|-------:|--------:|:-----------------------|
#  |  1 | HIC     |   2021 | 1.27667 | Países de ingreso alto |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='valor')
#  RangeIndex: 11 entries, 0 to 10
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3c      11 non-null     object 
#   1   anio       11 non-null     int64  
#   2   valor      11 non-null     float64
#   3   categoria  11 non-null     object 
#  
#  |    | iso3c   |   anio |   valor | categoria   |
#  |---:|:--------|-------:|--------:|:------------|
#  |  0 | BOL     |   2021 | 12.9229 | Bolivia     |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3c', 'anio'], axis=1)
#  RangeIndex: 11 entries, 0 to 10
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   valor      11 non-null     float64
#   1   categoria  11 non-null     object 
#  
#  |    |   valor | categoria   |
#  |---:|--------:|:------------|
#  |  0 | 12.9229 | Bolivia     |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Mundo', new_value='Media mundial')
#  RangeIndex: 11 entries, 0 to 10
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   valor      11 non-null     float64
#   1   categoria  11 non-null     object 
#  
#  |    |   valor | categoria   |
#  |---:|--------:|:------------|
#  |  0 | 12.9229 | Bolivia     |
#  
#  ------------------------------
#  