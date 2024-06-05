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
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          825 non-null    int64  
#   1   iso3          825 non-null    object 
#   2   valor_en_twh  612 non-null    float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1990 | ARG    |            nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          825 non-null    int64  
#   1   iso3          825 non-null    object 
#   2   valor_en_twh  612 non-null    float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1990 | ARG    |            nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          825 non-null    int64  
#   1   iso3          825 non-null    object 
#   2   valor_en_twh  612 non-null    float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1990 | ARG    |            nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       825 non-null    int64  
#   1   geocodigo  825 non-null    object 
#   2   valor      612 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1990 | ARG         |     nan |
#  
#  ------------------------------
#  