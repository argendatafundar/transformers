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
replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12744 entries, 0 to 12743
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12744 non-null  int64  
#   1   iso3          12744 non-null  object 
#   2   valor_en_twh  7423 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   2000 | AFG    |           0.31 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 12744 entries, 0 to 12743
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12744 non-null  int64  
#   1   iso3          12744 non-null  object 
#   2   valor_en_twh  7423 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   2000 | AFG    |           0.31 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 12744 entries, 0 to 12743
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12744 non-null  int64  
#   1   iso3          12744 non-null  object 
#   2   valor_en_twh  7423 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   2000 | AFG    |           0.31 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 12744 entries, 0 to 12743
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12744 non-null  int64  
#   1   geocodigo  12744 non-null  object 
#   2   valor      7423 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2000 | AFG         |    0.31 |
#  
#  ------------------------------
#  