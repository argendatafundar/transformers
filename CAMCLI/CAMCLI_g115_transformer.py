from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long_pl(df: pl.DataFrame,
                    index,
                    variable_name,
                    value_name,
                    on = None):

    df = df.unpivot(on = on, index = index, variable_name = variable_name, value_name = value_name)
    return df

@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'geocodigoFundar': 'geocodigo'}),
	drop_cols(cols=['anio']),
	wide_to_long_pl(index=['geonombreFundar'], variable_name='indicador', value_name='valor', on=['valor_en_mtco2e_per_cap', 'vab_precios_basicos_2004_per_cap']),
	replace_value(col='indicador', mapping={'valor_en_mtco2e_per_cap': 'Emisiones per cápita', 'vab_precios_basicos_2004_per_cap': 'PIB per cápita (precios 2004)'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_cols(cols=['anio'])
#  
#  ------------------------------
#  
#  wide_to_long_pl(index=['geonombreFundar'], variable_name='indicador', value_name='valor', on=['valor_en_mtco2e_per_cap', 'vab_precios_basicos_2004_per_cap'])
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'valor_en_mtco2e_per_cap': 'Emisiones per cápita', 'vab_precios_basicos_2004_per_cap': 'PIB per cápita (precios 2004)'}, alias=None)
#  
#  ------------------------------
#  