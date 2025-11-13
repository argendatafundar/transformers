from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df: pl.DataFrame, by='anio'):
    latest_year = df[by].max()
    df = df.filter(pl.col(by) == latest_year)
    df = df.drop(by)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    return df.rename(map)

@transformer.convert
def drop_col(df: pl.DataFrame, col, axis=1):
    # In Polars, axis parameter is not needed
    if isinstance(col, str):
        return df.drop(col)
    elif isinstance(col, list):
        return df.drop(col)
    else:
        return df.drop([col])

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	latest_year(by='anio'),
	rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'valor_en_porcent': 'valor'}),
	drop_col(col=['valor_en_mtco2e'], axis=1),
	replace_value(col='nivel1', mapping={'Procesos industriales y uso de productos': 'PIUP', 'Agricultura, ganadería, silvicultura y otros usos de la tierra': 'AGSyOUT'}, alias=None),
	replace_value(col='nivel2', mapping={'Industrias de la energía': 'Energía', 'Industrias manufactureras y de la construcción': 'Industria y construcción', 'Emisiones fugitivas provenientes de la fabricación de combustibles': 'Emisiones fugitivas'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'valor_en_porcent': 'valor'})
#  
#  ------------------------------
#  
#  drop_col(col=['valor_en_mtco2e'], axis=1)
#  
#  ------------------------------
#  
#  replace_value(col='nivel1', mapping={'Procesos industriales y uso de productos': 'PIUP', 'Agricultura, ganadería, silvicultura y otros usos de la tierra': 'AGSyOUT'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='nivel2', mapping={'Industrias de la energía': 'Energía', 'Industrias manufactureras y de la construcción': 'Industria y construcción', 'Emisiones fugitivas provenientes de la fabricación de combustibles': 'Emisiones fugitivas'}, alias=None)
#  
#  ------------------------------
#  