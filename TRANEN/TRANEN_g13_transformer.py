from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def drop_na(df: pl.DataFrame, cols: list):
    return df.drop_nulls(subset=cols)

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'tipo_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'}),
	df_sql(query="select * from self where indicador != 'Total' and geocodigo = 'ARG'"),
	df_sql(query="select * from self where indicador != 'Otras renovables'"),
	drop_na(cols=['valor']),
	drop_cols(cols=['valor_en_twh']),
	replace_value(col='indicador', mapping={'Bioenergia': 'Bioenergía', 'Carbon': 'Carbón', 'Petroleo': 'Petróleo', 'Eolica': 'Eólica'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where indicador != 'Total' and geocodigo = 'ARG'")
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where indicador != 'Otras renovables'")
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  drop_cols(cols=['valor_en_twh'])
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'Bioenergia': 'Bioenergía', 'Carbon': 'Carbón', 'Petroleo': 'Petróleo', 'Eolica': 'Eólica'}, alias=None)
#  
#  ------------------------------
#  