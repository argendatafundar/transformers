from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(mapping=map)
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
def identity(df: DataFrame, dummy = True) -> DataFrame:
    return df

@transformer.convert
def sort_values(df: pl.DataFrame, by, descending = None):
    if not descending:
        descending = [False] * len(by)
    df = df.sort(by = by, descending= descending)
    return df

@transformer.convert
def round(df: pl.DataFrame, col, digits):
    df = df.with_columns([
        pl.col(col).round(digits).alias(col)
    ])
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity(dummy=True),
	sort_values(by=['geonombreFundar', 'sector'], descending=True),
	rename_cols(map={'geonombreFundar': 'x', 'sector': 'categoria', 'valor_en_porcent': 'y'}),
	replace_value(col='categoria', mapping={'Procesos industriales y uso de productos': 'PIUP', 'AGSyOUT': 'AGSyOUT '}, alias=None),
	round(col='y', digits=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  identity(dummy=True)
#  
#  ------------------------------
#  
#  sort_values(by=['geonombreFundar', 'sector'], descending=True)
#  
#  ------------------------------
#  
#  rename_cols(map={'geonombreFundar': 'x', 'sector': 'categoria', 'valor_en_porcent': 'y'})
#  
#  ------------------------------
#  
#  replace_value(col='categoria', mapping={'Procesos industriales y uso de productos': 'PIUP', 'AGSyOUT': 'AGSyOUT '}, alias=None)
#  
#  ------------------------------
#  
#  round(col='y', digits=1)
#  
#  ------------------------------
#  