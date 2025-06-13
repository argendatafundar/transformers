from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df

@transformer.convert
def sort_values(df: pl.DataFrame, by, descending = None):
    if not descending:
        descending = [False] * len(by)
    df = df.sort(by = by, descending= descending)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='iso3', mapping={'OWID_WRL': 'WLD', 'OWID_KOS': 'XKX'}, alias=None),
	rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(by=['geocodigo', 'anio'], descending=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='iso3', mapping={'OWID_WRL': 'WLD', 'OWID_KOS': 'XKX'}, alias=None)
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  sort_values(by=['geocodigo', 'anio'], descending=None)
#  
#  ------------------------------
#  