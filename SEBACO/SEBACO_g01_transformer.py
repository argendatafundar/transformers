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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='sector', axis=1),
	rename_cols(map={'share_ventas': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          8 non-null      int64  
#   1   sector        8 non-null      object 
#   2   share_ventas  8 non-null      float64
#  
#  |    |   anio | sector   |   share_ventas |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2014 | SBC      |      0.0259628 |
#  
#  ------------------------------
#  
#  drop_col(col='sector', axis=1)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          8 non-null      int64  
#   1   share_ventas  8 non-null      float64
#  
#  |    |   anio |   share_ventas |
#  |---:|-------:|---------------:|
#  |  0 |   2014 |      0.0259628 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share_ventas': 'valor'})
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    8 non-null      int64  
#   1   valor   8 non-null      float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2014 | 2.59628 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    8 non-null      int64  
#   1   valor   8 non-null      float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2014 | 2.59628 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    8 non-null      int64  
#   1   valor   8 non-null      float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2014 | 2.59628 |
#  
#  ------------------------------
#  