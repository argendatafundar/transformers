from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def pandas_drop_na(df, cols: list):
    df = df.dropna(subset=cols)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition="geocodigoFundar == 'ARG'"),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='geonombreFundar', axis=1),
	rename_cols(map={'exportsofgoodsandservicesofgdp': 'valor'}),
	sort_values(how='ascending', by=['anio']),
	pandas_drop_na(cols=['valor'])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geocodigoFundar                 16960 non-null  object 
#   1   geonombreFundar                 16960 non-null  object 
#   2   anio                            16960 non-null  int64  
#   3   countryname                     16960 non-null  object 
#   4   exportsofgoodsandservicesofgdp  10756 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | countryname   |   exportsofgoodsandservicesofgdp |
#  |---:|:------------------|:------------------|-------:|:--------------|---------------------------------:|
#  |  0 | ABW               | Aruba             |   2023 | Aruba         |                              nan |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 64 entries, 576 to 639
#  Data columns (total 5 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geocodigoFundar                 64 non-null     object 
#   1   geonombreFundar                 64 non-null     object 
#   2   anio                            64 non-null     int64  
#   3   countryname                     64 non-null     object 
#   4   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | countryname   |   exportsofgoodsandservicesofgdp |
#  |----:|:------------------|:------------------|-------:|:--------------|---------------------------------:|
#  | 576 | ARG               | Argentina         |   2023 | Argentina     |                              nan |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 64 entries, 576 to 639
#  Data columns (total 4 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geonombreFundar                 64 non-null     object 
#   1   anio                            64 non-null     int64  
#   2   countryname                     64 non-null     object 
#   3   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |     | geonombreFundar   |   anio | countryname   |   exportsofgoodsandservicesofgdp |
#  |----:|:------------------|-------:|:--------------|---------------------------------:|
#  | 576 | Argentina         |   2023 | Argentina     |                              nan |
#  
#  ------------------------------
#  
#  drop_col(col='geonombreFundar', axis=1)
#  Index: 64 entries, 576 to 639
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   anio                            64 non-null     int64  
#   1   countryname                     64 non-null     object 
#   2   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |     |   anio | countryname   |   exportsofgoodsandservicesofgdp |
#  |----:|-------:|:--------------|---------------------------------:|
#  | 576 |   2023 | Argentina     |                              nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'exportsofgoodsandservicesofgdp': 'valor'})
#  Index: 64 entries, 576 to 639
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         64 non-null     int64  
#   1   countryname  64 non-null     object 
#   2   valor        63 non-null     float64
#  
#  |     |   anio | countryname   |   valor |
#  |----:|-------:|:--------------|--------:|
#  | 576 |   2023 | Argentina     |     nan |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         64 non-null     int64  
#   1   countryname  64 non-null     object 
#   2   valor        63 non-null     float64
#  
#  |    |   anio | countryname   |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1960 | Argentina     | 7.60405 |
#  
#  ------------------------------
#  
#  pandas_drop_na(cols=['valor'])
#  Index: 63 entries, 0 to 62
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         63 non-null     int64  
#   1   countryname  63 non-null     object 
#   2   valor        63 non-null     float64
#  
#  |    |   anio | countryname   |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1960 | Argentina     | 7.60405 |
#  
#  ------------------------------
#  