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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	query(condition='anio == 2020'),
	rename_cols(map={'iso3': 'geocodigo', 'valor_en_gco2_por_kwh': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7310 entries, 0 to 7309
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7310 non-null   int64  
#   1   iso3                   7310 non-null   object 
#   2   valor_en_gco2_por_kwh  5318 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   2000 | AFG    |                     250 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 7310 entries, 0 to 7309
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7310 non-null   int64  
#   1   iso3                   7310 non-null   object 
#   2   valor_en_gco2_por_kwh  5318 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   2000 | AFG    |                     250 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 7310 entries, 0 to 7309
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7310 non-null   int64  
#   1   iso3                   7310 non-null   object 
#   2   valor_en_gco2_por_kwh  5318 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   2000 | AFG    |                     250 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2020')
#  Index: 215 entries, 20 to 7296
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   215 non-null    int64  
#   1   iso3                   215 non-null    object 
#   2   valor_en_gco2_por_kwh  214 non-null    float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  | 20 |   2020 | AFG    |                     125 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'valor_en_gco2_por_kwh': 'valor'})
#  Index: 215 entries, 20 to 7296
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       215 non-null    int64  
#   1   geocodigo  215 non-null    object 
#   2   valor      214 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  | 20 |   2020 | AFG         |     125 |
#  
#  ------------------------------
#  