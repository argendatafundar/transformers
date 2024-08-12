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
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'}),
	drop_col(col='pais', axis=1),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16994 entries, 0 to 16993
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              16994 non-null  object 
#   1   anio               16960 non-null  float64
#   2   va_agro_sobre_pbi  10772 non-null  float64
#   3   pais               16738 non-null  object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais   |
#  |---:|:--------|-------:|--------------------:|:-------|
#  |  0 | ABW     |   1965 |                 nan | Aruba  |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'})
#  RangeIndex: 16994 entries, 0 to 16993
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16994 non-null  object 
#   1   anio       16960 non-null  float64
#   2   valor      10772 non-null  float64
#   3   pais       16738 non-null  object 
#  
#  |    | geocodigo   |   anio |   valor | pais   |
#  |---:|:------------|-------:|--------:|:-------|
#  |  0 | ABW         |   1965 |     nan | Aruba  |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 16994 entries, 0 to 16993
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16994 non-null  object 
#   1   anio       16960 non-null  float64
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1965 |     nan |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 10772 entries, 8 to 16988
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  float64
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |     valor |
#  |---:|:------------|-------:|----------:|
#  |  8 | ABW         |   2008 | 0.0169974 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   anio       10772 non-null  float64
#   2   valor      10772 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | BEN         |   1960 | 46.1577 |
#  
#  ------------------------------
#  