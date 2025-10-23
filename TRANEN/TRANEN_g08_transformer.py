from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: pl.DataFrame, by, descending):
    df = df.sort(by = by, descending= descending)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	df_sql(query="SELECT * from self WHERE fuente_energia = 'Solar'"),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(by=['geocodigo', 'anio'], descending=[False, False])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * from self WHERE fuente_energia = 'Solar'")
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  sort_values(by=['geocodigo', 'anio'], descending=[False, False])
#  
#  ------------------------------
#  