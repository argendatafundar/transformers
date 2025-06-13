from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df

@transformer.convert
def sort_values(df: pl.DataFrame, by, descending = None):
    if not descending:
        descending = [False] * len(by)
    df = df.sort(by = by, descending= descending)
    return df

@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
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
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='iso3', mapping={'OWID_WRL': 'WLD'}, alias=None),
	df_sql(query="SELECT * FROM self WHERE iso3 = 'WLD'"),
	df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Gas natural'"),
	df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Petróleo'"),
	df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Carbón'"),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(by=['anio'], descending=[False])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='iso3', mapping={'OWID_WRL': 'WLD'}, alias=None)
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * FROM self WHERE iso3 = 'WLD'")
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Gas natural'")
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Petróleo'")
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * FROM self WHERE fuente_energia != 'Carbón'")
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  sort_values(by=['anio'], descending=[False])
#  
#  ------------------------------
#  