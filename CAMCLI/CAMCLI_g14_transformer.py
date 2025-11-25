from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def pl_filter(df: pl.DataFrame, query: str):
    df = df.filter(eval(query))
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
	pl_filter(query="pl.col('anio') == max(df['anio'])"),
	rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'}),
	drop_cols(cols=['anio']),
	sort_values(by=['geonombreFundar', 'indicador'], descending=None),
	replace_value(col='geonombreFundar', mapping={'Santiago del Estero': 'S. del Estero', 'Tierra del Fuego': 'T. del Fuego'}, alias=None),
	replace_value(col='indicador', mapping={'AGSyOUT': 'AGSyOUT '}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  pl_filter(query="pl.col('anio') == max(df['anio'])")
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'})
#  
#  ------------------------------
#  
#  drop_cols(cols=['anio'])
#  
#  ------------------------------
#  
#  sort_values(by=['geonombreFundar', 'indicador'], descending=None)
#  
#  ------------------------------
#  
#  replace_value(col='geonombreFundar', mapping={'Santiago del Estero': 'S. del Estero', 'Tierra del Fuego': 'T. del Fuego'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'AGSyOUT': 'AGSyOUT '}, alias=None)
#  
#  ------------------------------
#  