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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='countryname', axis=1),
	wide_to_long(primary_keys=['anio', 'iso3'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='exportsconstant_goods_v2', new_value='Bienes'),
	replace_value(col='indicador', curr_value='exportsconstant_servi_v2', new_value='Servicios'),
	query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1)
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
#  |    |   anio | iso3   | countryname                 |   exportsconstant_goods_v2 |   exportsconstant_servi_v2 |
#  |---:|-------:|:-------|:----------------------------|---------------------------:|---------------------------:|
#  |  0 |   2023 | AFE    | Africa Eastern and Southern |                        nan |                        nan |
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
#  |  0 |   2023 | AFE    |                        nan |                        nan |
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
#  |  0 |   2023 | AFE    | exportsconstant_goods_v2 |     nan |
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
#  |  0 |   2023 | AFE    | Bienes      |     nan |
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
#  |  0 |   2023 | AFE    | Bienes      |     nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 128 entries, 3520 to 20543
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       128 non-null    int64  
#   1   iso3       128 non-null    object 
#   2   indicador  128 non-null    object 
#   3   valor      94 non-null     float64
#  
#  |      |   anio | iso3   | indicador   |   valor |
#  |-----:|-------:|:-------|:------------|--------:|
#  | 3520 |   2023 | ARG    | Bienes      |     nan |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 128 entries, 3520 to 20543
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       128 non-null    int64  
#   1   indicador  128 non-null    object 
#   2   valor      94 non-null     float64
#  
#  |      |   anio | indicador   |   valor |
#  |-----:|-------:|:------------|--------:|
#  | 3520 |   2023 | Bienes      |     nan |
#  
#  ------------------------------
#  