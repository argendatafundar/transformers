from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2021'),
	query(condition="~ iso3c.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])"),
	rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'}),
	sort_values(how='descending', by='valor'),
	drop_col(col=['anio', 'pais'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              10772 non-null  object 
#   1   anio               10772 non-null  int64  
#   2   va_agro_sobre_pbi  10772 non-null  float64
#   3   pais               10772 non-null  object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais   |
#  |---:|:--------|-------:|--------------------:|:-------|
#  |  0 | ABW     |   1995 |            0.505922 | Aruba  |
#  
#  ------------------------------
#  
#  query(condition='anio == 2021')
#  Index: 240 entries, 65 to 10769
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              240 non-null    object 
#   1   anio               240 non-null    int64  
#   2   va_agro_sobre_pbi  240 non-null    float64
#   3   pais               240 non-null    object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais                         |
#  |---:|:--------|-------:|--------------------:|:-----------------------------|
#  | 65 | AFE     |   2021 |             13.4028 | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  query(condition="~ iso3c.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])")
#  Index: 229 entries, 65 to 10769
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              229 non-null    object 
#   1   anio               229 non-null    int64  
#   2   va_agro_sobre_pbi  229 non-null    float64
#   3   pais               229 non-null    object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais                         |
#  |---:|:--------|-------:|--------------------:|:-----------------------------|
#  | 65 | AFE     |   2021 |             13.4028 | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'})
#  Index: 229 entries, 65 to 10769
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  229 non-null    object 
#   1   anio       229 non-null    int64  
#   2   valor      229 non-null    float64
#   3   pais       229 non-null    object 
#  
#  |    | geocodigo   |   anio |   valor | pais                         |
#  |---:|:------------|-------:|--------:|:-----------------------------|
#  | 65 | AFE         |   2021 | 13.4028 | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='valor')
#  RangeIndex: 229 entries, 0 to 228
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  229 non-null    object 
#   1   anio       229 non-null    int64  
#   2   valor      229 non-null    float64
#   3   pais       229 non-null    object 
#  
#  |    | geocodigo   |   anio |   valor | pais         |
#  |---:|:------------|-------:|--------:|:-------------|
#  |  0 | SLE         |   2021 | 57.4488 | Sierra Leona |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'pais'], axis=1)
#  RangeIndex: 229 entries, 0 to 228
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  229 non-null    object 
#   1   valor      229 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | SLE         | 57.4488 |
#  
#  ------------------------------
#  