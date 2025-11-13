from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def pl_filter(df: pl.DataFrame, query: str):
    df = df.filter(eval(query))
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
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
	sort_values(by=['geonombreFundar', 'indicador'], descending=None)
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