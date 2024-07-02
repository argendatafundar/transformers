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

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	query(condition='iso3 == "ARG" & tipo_energia != "Total"'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	drop_col(col=['iso3', 'porcentaje'], axis=1),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'indicador'])
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
#   3   valor_en_twh  57843 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   1985 | GBR    | Otras renovables |              0 |            0 |
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
#   3   valor_en_twh  57843 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   1985 | GBR    | Otras renovables |              0 |            0 |
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
#   3   valor_en_twh  57843 non-null  float64
#   4   porcentaje    81900 non-null  float64
#  
#  |    |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|
#  |  0 |   1985 | GBR    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG" & tipo_energia != "Total"')
#  Index: 351 entries, 390 to 65948
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          351 non-null    int64  
#   1   iso3          351 non-null    object 
#   2   tipo_energia  351 non-null    object 
#   3   valor_en_twh  351 non-null    float64
#   4   porcentaje    351 non-null    float64
#  
#  |     |   anio | iso3   | tipo_energia     |   valor_en_twh |   porcentaje |
#  |----:|-------:|:-------|:-----------------|---------------:|-------------:|
#  | 390 |   1985 | ARG    | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 351 entries, 390 to 65948
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        351 non-null    int64  
#   1   iso3        351 non-null    object 
#   2   indicador   351 non-null    object 
#   3   valor       351 non-null    float64
#   4   porcentaje  351 non-null    float64
#  
#  |     |   anio | iso3   | indicador        |   valor |   porcentaje |
#  |----:|-------:|:-------|:-----------------|--------:|-------------:|
#  | 390 |   1985 | ARG    | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'porcentaje'], axis=1)
#  Index: 351 entries, 390 to 65948
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       351 non-null    int64  
#   1   indicador  351 non-null    object 
#   2   valor      351 non-null    float64
#  
#  |     |   anio | indicador        |   valor |
#  |----:|-------:|:-----------------|--------:|
#  | 390 |   1985 | Otras renovables |       0 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 351 entries, 390 to 65948
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       351 non-null    int64  
#   1   indicador  351 non-null    object 
#   2   valor      351 non-null    float64
#  
#  |     |   anio | indicador        |   valor |
#  |----:|-------:|:-----------------|--------:|
#  | 390 |   1985 | Otras renovables |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  Index: 351 entries, 8580 to 16808
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       351 non-null    int64  
#   1   indicador  351 non-null    object 
#   2   valor      351 non-null    float64
#  
#  |      |   anio | indicador   |   valor |
#  |-----:|-------:|:------------|--------:|
#  | 8580 |   1985 | Bioenergia  |       0 |
#  
#  ------------------------------
#  