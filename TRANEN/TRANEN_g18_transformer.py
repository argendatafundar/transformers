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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'})
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
#  rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 7310 entries, 0 to 7309
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7310 non-null   int64  
#   1   geocodigo  7310 non-null   object 
#   2   valor      5318 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2000 | AFG         |     250 |
#  
#  ------------------------------
#  