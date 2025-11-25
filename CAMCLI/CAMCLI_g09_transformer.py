from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'valor_en_mtco2e': 'valor'}),
	replace_value(col='sector', mapping={'Procesos industriales y uso de productos': 'PIUP'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor'})
#  
#  ------------------------------
#  
#  replace_value(col='sector', mapping={'Procesos industriales y uso de productos': 'PIUP'}, alias=None)
#  
#  ------------------------------
#  