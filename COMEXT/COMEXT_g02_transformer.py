from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict):
    mapcol = colname+'_map'
    df.loc[:, mapcol] = df[colname].map(precedence)
    df = df.sort_values(by=['anio', mapcol])
    return df.drop(mapcol, axis=1)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='countryname', axis=1),
	wide_to_long(primary_keys=['anio', 'geonombreFundar'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='exportsconstant_goods_v2', new_value='Bienes'),
	replace_value(col='indicador', curr_value='exportsconstant_servi_v2', new_value='Servicios'),
	query(condition='geonombreFundar == "Argentina"'),
	drop_col(col='geonombreFundar', axis=1),
	drop_na(col='valor'),
	sort_values_by_comparison(colname='indicador', precedence={'Bienes': 0, 'Servicios': 1})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 6 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geocodigoFundar           16960 non-null  object 
#   1   geonombreFundar           16960 non-null  object 
#   2   anio                      16960 non-null  int64  
#   3   countryname               16960 non-null  object 
#   4   exportsconstant_goods_v2  6251 non-null   float64
#   5   exportsconstant_servi_v2  6251 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | countryname   |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|:------------------|:------------------|-------:|:--------------|---------------------------:|---------------------------:|
#  |  0 | ABW               | Aruba             |   2023 | Aruba         |                        nan |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geonombreFundar           16960 non-null  object 
#   1   anio                      16960 non-null  int64  
#   2   countryname               16960 non-null  object 
#   3   exportsconstant_goods_v2  6251 non-null   float64
#   4   exportsconstant_servi_v2  6251 non-null   float64
#  
#  |    | geonombreFundar   |   anio | countryname   |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|:------------------|-------:|:--------------|---------------------------:|---------------------------:|
#  |  0 | Aruba             |   2023 | Aruba         |                        nan |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   geonombreFundar           16960 non-null  object 
#   1   anio                      16960 non-null  int64  
#   2   exportsconstant_goods_v2  6251 non-null   float64
#   3   exportsconstant_servi_v2  6251 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|:------------------|-------:|---------------------------:|---------------------------:|
#  |  0 | Aruba             |   2023 |                        nan |                        nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'geonombreFundar'], value_name='valor', var_name='indicador')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             33920 non-null  int64  
#   1   geonombreFundar  33920 non-null  object 
#   2   indicador        33920 non-null  object 
#   3   valor            12502 non-null  float64
#  
#  |    |   anio | geonombreFundar   | indicador                |   valor |
#  |---:|-------:|:------------------|:-------------------------|--------:|
#  |  0 |   2023 | Aruba             | exportsconstant_goods_v2 |     nan |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='exportsconstant_goods_v2', new_value='Bienes')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             33920 non-null  int64  
#   1   geonombreFundar  33920 non-null  object 
#   2   indicador        33920 non-null  object 
#   3   valor            12502 non-null  float64
#  
#  |    |   anio | geonombreFundar   | indicador   |   valor |
#  |---:|-------:|:------------------|:------------|--------:|
#  |  0 |   2023 | Aruba             | Bienes      |     nan |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='exportsconstant_servi_v2', new_value='Servicios')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             33920 non-null  int64  
#   1   geonombreFundar  33920 non-null  object 
#   2   indicador        33920 non-null  object 
#   3   valor            12502 non-null  float64
#  
#  |    |   anio | geonombreFundar   | indicador   |   valor |
#  |---:|-------:|:------------------|:------------|--------:|
#  |  0 |   2023 | Aruba             | Bienes      |     nan |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 128 entries, 576 to 17599
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             128 non-null    int64  
#   1   geonombreFundar  128 non-null    object 
#   2   indicador        128 non-null    object 
#   3   valor            94 non-null     float64
#  
#  |     |   anio | geonombreFundar   | indicador   |   valor |
#  |----:|-------:|:------------------|:------------|--------:|
#  | 576 |   2023 | Argentina         | Bienes      |     nan |
#  
#  ------------------------------
#  
#  drop_col(col='geonombreFundar', axis=1)
#  Index: 128 entries, 576 to 17599
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       128 non-null    int64  
#   1   indicador  128 non-null    object 
#   2   valor      94 non-null     float64
#  
#  |     |   anio | indicador   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 576 |   2023 | Bienes      |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 94 entries, 577 to 17599
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           94 non-null     int64  
#   1   indicador      94 non-null     object 
#   2   valor          94 non-null     float64
#   3   indicador_map  94 non-null     int64  
#  
#  |     |   anio | indicador   |   valor |   indicador_map |
#  |----:|-------:|:------------|--------:|----------------:|
#  | 577 |   2022 | Bienes      | 61887.4 |               0 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Bienes': 0, 'Servicios': 1})
#  Index: 94 entries, 622 to 17537
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       94 non-null     int64  
#   1   indicador  94 non-null     object 
#   2   valor      94 non-null     float64
#  
#  |     |   anio | indicador   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 622 |   1976 | Bienes      | 9548.58 |
#  
#  ------------------------------
#  