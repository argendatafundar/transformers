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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='anio', axis=1),
	drop_col(col='pais_desc', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'estado': 'indicador', 'satisfaccion_vida': 'valor'}),
	sort_values(how='ascending', by=['geocodigo', 'indicador'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               193 non-null    object 
#   1   pais_desc          193 non-null    object 
#   2   anio               193 non-null    int64  
#   3   estado             193 non-null    object 
#   4   satisfaccion_vida  193 non-null    float64
#  
#  |    | iso3   | pais_desc   |   anio | estado     |   satisfaccion_vida |
#  |---:|:-------|:------------|-------:|:-----------|--------------------:|
#  |  0 | ALB    | Albania     |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               193 non-null    object 
#   1   pais_desc          193 non-null    object 
#   2   estado             193 non-null    object 
#   3   satisfaccion_vida  193 non-null    float64
#  
#  |    | iso3   | pais_desc   | estado     |   satisfaccion_vida |
#  |---:|:-------|:------------|:-----------|--------------------:|
#  |  0 | ALB    | Albania     | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               193 non-null    object 
#   1   estado             193 non-null    object 
#   2   satisfaccion_vida  193 non-null    float64
#  
#  |    | iso3   | estado     |   satisfaccion_vida |
#  |---:|:-------|:-----------|--------------------:|
#  |  0 | ALB    | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'estado': 'indicador', 'satisfaccion_vida': 'valor'})
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  193 non-null    object 
#   1   indicador  193 non-null    object 
#   2   valor      193 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | ALB         | Desocupado  |   6.642 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['geocodigo', 'indicador'])
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  193 non-null    object 
#   1   indicador  193 non-null    object 
#   2   valor      193 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | ALB         | Desocupado  |   6.642 |
#  
#  ------------------------------
#  