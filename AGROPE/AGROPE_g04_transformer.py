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
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'}),
	drop_col(col='pais', axis=1),
	drop_na(subset=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])"),
	query(condition='anio <= 2022')
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
#  rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'})
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  int64  
#   2   valor      10772 non-null  float64
#   3   pais       10772 non-null  object 
#  
#  |    | geocodigo   |   anio |    valor | pais   |
#  |---:|:------------|-------:|---------:|:-------|
#  |  0 | ABW         |   1995 | 0.505922 | Aruba  |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  int64  
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | ABW         |   1995 | 0.505922 |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  int64  
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | ABW         |   1995 | 0.505922 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  int64  
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | BEN         |   1960 | 46.1577 |
#  
#  ------------------------------
#  
#  query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])")
#  Index: 10290 entries, 0 to 10771
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10290 non-null  object 
#   1   anio       10290 non-null  int64  
#   2   valor      10290 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | BEN         |   1960 | 46.1577 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2022')
#  Index: 10106 entries, 0 to 10576
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10106 non-null  object 
#   1   anio       10106 non-null  int64  
#   2   valor      10106 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | BEN         |   1960 | 46.1577 |
#  
#  ------------------------------
#  