from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="variable in ('ila','suavizado')"),
	long_to_wide(index=['anosedu'], columns='variable', values='valor'),
	sort_values(how='ascending', by=['anosedu'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anosedu   42 non-null     int64  
#   1   variable  42 non-null     object 
#   2   valor     42 non-null     float64
#  
#  |    |   anosedu | variable   |   valor |
#  |---:|----------:|:-----------|--------:|
#  |  0 |         0 | ila        |  194237 |
#  
#  ------------------------------
#  
#  query(condition="variable in ('ila','suavizado')")
#  Index: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anosedu   42 non-null     int64  
#   1   variable  42 non-null     object 
#   2   valor     42 non-null     float64
#  
#  |    |   anosedu | variable   |   valor |
#  |---:|----------:|:-----------|--------:|
#  |  0 |         0 | ila        |  194237 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['anosedu'], columns='variable', values='valor')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anosedu    21 non-null     int64  
#   1   ila        21 non-null     float64
#   2   suavizado  21 non-null     float64
#  
#  |    |   anosedu |    ila |   suavizado |
#  |---:|----------:|-------:|------------:|
#  |  0 |         0 | 194237 |      187810 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anosedu'])
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anosedu    21 non-null     int64  
#   1   ila        21 non-null     float64
#   2   suavizado  21 non-null     float64
#  
#  |    |   anosedu |    ila |   suavizado |
#  |---:|----------:|-------:|------------:|
#  |  0 |         0 | 194237 |      187810 |
#  
#  ------------------------------
#  