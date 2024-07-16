from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['anio', 'provincia'], value_name='valor', var_name='indicador'),
	rename_cols(map={'provincia': 'categoria'}),
	drop_col(col='anio', axis=1),
	replace_value(col='indicador', curr_value='valor_en_mtco2e_per_cap', new_value='Emisiones per cápita'),
	replace_value(col='indicador', curr_value='vab_precios_basicos_2004_per_cap', new_value='PIB per cápita (precios 2004)')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   anio                              24 non-null     int64  
#   1   provincia                         24 non-null     object 
#   2   valor_en_mtco2e_per_cap           24 non-null     float64
#   3   vab_precios_basicos_2004_per_cap  24 non-null     float64
#  
#  |    |   anio | provincia    |   valor_en_mtco2e_per_cap |   vab_precios_basicos_2004_per_cap |
#  |---:|-------:|:-------------|--------------------------:|-----------------------------------:|
#  |  0 |   2018 | Buenos Aires |                      5.45 |                              11034 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'provincia'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       48 non-null     int64  
#   1   provincia  48 non-null     object 
#   2   indicador  48 non-null     object 
#   3   valor      48 non-null     float64
#  
#  |    |   anio | provincia    | indicador               |   valor |
#  |---:|-------:|:-------------|:------------------------|--------:|
#  |  0 |   2018 | Buenos Aires | valor_en_mtco2e_per_cap |    5.45 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'categoria'})
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       48 non-null     int64  
#   1   categoria  48 non-null     object 
#   2   indicador  48 non-null     object 
#   3   valor      48 non-null     float64
#  
#  |    |   anio | categoria    | indicador               |   valor |
#  |---:|-------:|:-------------|:------------------------|--------:|
#  |  0 |   2018 | Buenos Aires | valor_en_mtco2e_per_cap |    5.45 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Buenos Aires | valor_en_mtco2e_per_cap |    5.45 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='valor_en_mtco2e_per_cap', new_value='Emisiones per cápita')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | categoria    | indicador            |   valor |
#  |---:|:-------------|:---------------------|--------:|
#  |  0 | Buenos Aires | Emisiones per cápita |    5.45 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='vab_precios_basicos_2004_per_cap', new_value='PIB per cápita (precios 2004)')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | categoria    | indicador            |   valor |
#  |---:|:-------------|:---------------------|--------:|
#  |  0 | Buenos Aires | Emisiones per cápita |    5.45 |
#  
#  ------------------------------
#  