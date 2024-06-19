from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	query(condition='iso3 == "ARG"'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	drop_col(col=['iso3', 'porcentaje'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81900 entries, 0 to 81899
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          81900 non-null  int64  
#   1   iso3          81900 non-null  object 
#   2   tipo_energia  81900 non-null  object 
#   3   valor_en_twh  57771 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   2000 | AFG    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 81900 entries, 0 to 81899
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          81900 non-null  int64  
#   1   iso3          81900 non-null  object 
#   2   tipo_energia  81900 non-null  object 
#   3   valor_en_twh  57771 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   2000 | AFG    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 81900 entries, 0 to 81899
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          81900 non-null  int64  
#   1   iso3          81900 non-null  object 
#   2   tipo_energia  81900 non-null  object 
#   3   valor_en_twh  57771 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   2000 | AFG    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 390 entries, 234 to 81695
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          390 non-null    int64  
#   1   iso3          390 non-null    object 
#   2   tipo_energia  390 non-null    object 
#   3   valor_en_twh  390 non-null    float64
#   4   porcentaje    390 non-null    float64
#  
#  |     |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |----:|-------:|:-------|:-----------------|---------------:|-------------:|
#  | 234 |   2000 | ARG    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 390 entries, 234 to 81695
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        390 non-null    int64  
#   1   iso3        390 non-null    object 
#   2   indicador   390 non-null    object 
#   3   valor       390 non-null    float64
#   4   porcentaje  390 non-null    float64
#  
#  |     |   anio | iso3   | indicador        |   valor |   porcentaje |
#  |----:|-------:|:-------|:-----------------|--------:|-------------:|
#  | 234 |   2000 | ARG    | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'porcentaje'], axis=1)
#  Index: 390 entries, 234 to 81695
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       390 non-null    int64  
#   1   indicador  390 non-null    object 
#   2   valor      390 non-null    float64
#  
#  |     |   anio | indicador        |   valor |
#  |----:|-------:|:-----------------|--------:|
#  | 234 |   2000 | Otras renovables |       0 |
#  
#  ------------------------------
#  