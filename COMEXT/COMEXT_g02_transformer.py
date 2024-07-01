from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_na(df: DataFrame, col:str):
    df = df.dropna(subset= col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='countryname', axis=1),
	wide_to_long(primary_keys=['anio', 'iso3'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='exportsconstant_goods_v2', new_value='Bienes'),
	replace_value(col='indicador', curr_value='exportsconstant_servi_v2', new_value='Servicios'),
	query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      16960 non-null  int64  
#   1   iso3                      16960 non-null  object 
#   2   countryname               16960 non-null  object 
#   3   exportsconstant_goods_v2  6251 non-null   float64
#   4   exportsconstant_servi_v2  6251 non-null   float64
#  
#  |    |   anio | iso3   | countryname   |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|-------:|:-------|:--------------|---------------------------:|---------------------------:|
#  |  0 |   2023 | ABW    | Aruba         |                        nan |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      16960 non-null  int64  
#   1   iso3                      16960 non-null  object 
#   2   exportsconstant_goods_v2  6251 non-null   float64
#   3   exportsconstant_servi_v2  6251 non-null   float64
#  
#  |    |   anio | iso3   |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|-------:|:-------|---------------------------:|---------------------------:|
#  |  0 |   2023 | ABW    |                        nan |                        nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'iso3'], value_name='valor', var_name='indicador')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       33920 non-null  int64  
#   1   iso3       33920 non-null  object 
#   2   indicador  33920 non-null  object 
#   3   valor      12502 non-null  float64
#  
#  |    |   anio | iso3   | indicador                |   valor |
#  |---:|-------:|:-------|:-------------------------|--------:|
#  |  0 |   2023 | ABW    | exportsconstant_goods_v2 |     nan |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='exportsconstant_goods_v2', new_value='Bienes')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       33920 non-null  int64  
#   1   iso3       33920 non-null  object 
#   2   indicador  33920 non-null  object 
#   3   valor      12502 non-null  float64
#  
#  |    |   anio | iso3   | indicador   |   valor |
#  |---:|-------:|:-------|:------------|--------:|
#  |  0 |   2023 | ABW    | Bienes      |     nan |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='exportsconstant_servi_v2', new_value='Servicios')
#  RangeIndex: 33920 entries, 0 to 33919
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       33920 non-null  int64  
#   1   iso3       33920 non-null  object 
#   2   indicador  33920 non-null  object 
#   3   valor      12502 non-null  float64
#  
#  |    |   anio | iso3   | indicador   |   valor |
#  |---:|-------:|:-------|:------------|--------:|
#  |  0 |   2023 | ABW    | Bienes      |     nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 128 entries, 576 to 17599
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       128 non-null    int64  
#   1   iso3       128 non-null    object 
#   2   indicador  128 non-null    object 
#   3   valor      94 non-null     float64
#  
#  |     |   anio | iso3   | indicador   |   valor |
#  |----:|-------:|:-------|:------------|--------:|
#  | 576 |   2023 | ARG    | Bienes      |     nan |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
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
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       94 non-null     int64  
#   1   indicador  94 non-null     object 
#   2   valor      94 non-null     float64
#  
#  |     |   anio | indicador   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 577 |   2022 | Bienes      | 61887.4 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       94 non-null     int64  
#   1   indicador  94 non-null     object 
#   2   valor      94 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1976 | Bienes      | 9548.58 |
#  
#  ------------------------------
#  