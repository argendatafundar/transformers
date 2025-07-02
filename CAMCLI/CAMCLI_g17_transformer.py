from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: pl.DataFrame, col: str, curr_value: str, new_value: str):
    df = df.with_columns(pl.col(col).replace(curr_value, new_value))
    return df

@transformer.convert
def mutiplicar_por_escalar(df: pl.DataFrame, col:str, k:float):
    df = df.with_columns(pl.col(col) * k)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def wide_to_long(df: pl.DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.unpivot(index=primary_keys, value_name=value_name, variable_name=var_name)

@transformer.convert
def datetime_to_year(df: pl.DataFrame, col: str):
    df = df.with_columns(pl.col(col).str.strptime(pl.Date).dt.year().alias(col))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	rename_cols(map={'fecha': 'anio'}),
	datetime_to_year(col='anio'),
	replace_value(col='indicador', curr_value='emisiones_anuales_co2_toneladas', new_value='Dióxido de carbono (CO2)'),
	replace_value(col='indicador', curr_value='emisiones_anuales_n2o_en_co2_toneladas', new_value='Óxido nitroso (N2O)'),
	replace_value(col='indicador', curr_value='emisiones_anuales_ch4_en_co2_toneladas', new_value='Metano (CH4)'),
	mutiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio'})
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_co2_toneladas', new_value='Dióxido de carbono (CO2)')
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_n2o_en_co2_toneladas', new_value='Óxido nitroso (N2O)')
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_ch4_en_co2_toneladas', new_value='Metano (CH4)')
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=1e-06)
#  
#  ------------------------------
#  