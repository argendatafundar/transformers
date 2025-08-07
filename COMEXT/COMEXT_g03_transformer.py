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
def drop_na(df: DataFrame, col:str):
    df = df.dropna(subset= col, axis=0)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	drop_col(col='countryname', axis=1),
	query(condition='geocodigoFundar == "ARG"'),
	drop_col(col='geocodigoFundar', axis=1),
	rename_cols(map={'servicesexportsbop_pc_v2': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geocodigoFundar           16960 non-null  object 
#   1   geonombreFundar           16960 non-null  object 
#   2   anio                      16960 non-null  int64  
#   3   countryname               16960 non-null  object 
#   4   servicesexportsbop_pc_v2  9077 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | countryname   |   servicesexportsbop_pc_v2 |
#  |---:|:------------------|:------------------|-------:|:--------------|---------------------------:|
#  |  0 | ABW               | Aruba             |   2023 | Aruba         |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geocodigoFundar           16960 non-null  object 
#   1   geonombreFundar           16960 non-null  object 
#   2   anio                      16960 non-null  int64  
#   3   servicesexportsbop_pc_v2  9077 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   servicesexportsbop_pc_v2 |
#  |---:|:------------------|:------------------|-------:|---------------------------:|
#  |  0 | ABW               | Aruba             |   2023 |                        nan |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 64 entries, 576 to 639
#  Data columns (total 4 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geocodigoFundar           64 non-null     object 
#   1   geonombreFundar           64 non-null     object 
#   2   anio                      64 non-null     int64  
#   3   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   servicesexportsbop_pc_v2 |
#  |----:|:------------------|:------------------|-------:|---------------------------:|
#  | 576 | ARG               | Argentina         |   2023 |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 64 entries, 576 to 639
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geonombreFundar           64 non-null     object 
#   1   anio                      64 non-null     int64  
#   2   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |     | geonombreFundar   |   anio |   servicesexportsbop_pc_v2 |
#  |----:|:------------------|-------:|---------------------------:|
#  | 576 | Argentina         |   2023 |                        nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'servicesexportsbop_pc_v2': 'valor'})
#  Index: 64 entries, 576 to 639
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  64 non-null     object 
#   1   anio             64 non-null     int64  
#   2   valor            47 non-null     float64
#  
#  |     | geonombreFundar   |   anio |   valor |
#  |----:|:------------------|-------:|--------:|
#  | 576 | Argentina         |   2023 |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 47 entries, 577 to 639
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  47 non-null     object 
#   1   anio             47 non-null     int64  
#   2   valor            47 non-null     float64
#  
#  |     | geonombreFundar   |   anio |   valor |
#  |----:|:------------------|-------:|--------:|
#  | 577 | Argentina         |   2022 | 14.0644 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 47 entries, 0 to 46
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  47 non-null     object 
#   1   anio             47 non-null     int64  
#   2   valor            47 non-null     float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Argentina         |   1976 | 15.4145 |
#  
#  ------------------------------
#  