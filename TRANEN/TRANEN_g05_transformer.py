from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: pl.DataFrame, by, descending):
    df = df.sort(by = by, descending= descending)
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
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    return df.drop_nans(subset=cols)

@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	df_sql(query="SELECT * FROM self WHERE tipo_energia != 'Total' AND geocodigoFundar = 'ARG'"),
	replace_value(col='fuente_energia', mapping={'Eolica': 'Eólica'}, alias=None),
	replace_value(col='fuente_energia', mapping={'Carbon': 'Carbón'}, alias=None),
	replace_value(col='fuente_energia', mapping={'Petroleo': 'Petróleo'}, alias=None),
	replace_value(col='geocodigoFundar', mapping={'OWID_WRL': 'WLD'}, alias=None),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'}),
	drop_cols(cols=['valor_en_twh', 'tipo_energia']),
	drop_na(cols=['valor']),
	replace_value(col='indicador', mapping={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, alias='orden'),
	sort_values(by=['orden', 'anio'], descending=[False, False])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  df_sql(query="SELECT * FROM self WHERE tipo_energia != 'Total' AND geocodigoFundar = 'ARG'")
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', mapping={'Eolica': 'Eólica'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', mapping={'Carbon': 'Carbón'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', mapping={'Petroleo': 'Petróleo'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='geocodigoFundar', mapping={'OWID_WRL': 'WLD'}, alias=None)
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_cols(cols=['valor_en_twh', 'tipo_energia'])
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, alias='orden')
#  
#  ------------------------------
#  
#  sort_values(by=['orden', 'anio'], descending=[False, False])
#  
#  ------------------------------
#  