from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def cast_to(df: pl.DataFrame, col: str, target_type: str = "pl.Float64") -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).cast(eval(target_type), strict=False)
    ])

@transformer.convert
def multiplicar_por_escalar(df: pl.DataFrame, col: str, k: float) -> pl.DataFrame:
    return df.with_columns([
        (pl.col(col) * k).alias(col)
    ])

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None),
	df_sql(query="select * from self where year = 2024 and semester = '1' and region != 'Total'"),
	cast_to(col='k_value', target_type='pl.String'),
	replace_value(col='k_value', mapping={0.25: 'k = 0,25', 0.35: 'k = 0,35'}, alias=None),
	replace_value(col='region', mapping={'Partidos': 'Partidos GBA'}, alias=None),
	multiplicar_por_escalar(col='pov_rate', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None)
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where year = 2024 and semester = '1' and region != 'Total'")
#  
#  ------------------------------
#  
#  cast_to(col='k_value', target_type='pl.String')
#  
#  ------------------------------
#  
#  replace_value(col='k_value', mapping={0.25: 'k = 0,25', 0.35: 'k = 0,35'}, alias=None)
#  
#  ------------------------------
#  
#  replace_value(col='region', mapping={'Partidos': 'Partidos GBA'}, alias=None)
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='pov_rate', k=100)
#  
#  ------------------------------
#  