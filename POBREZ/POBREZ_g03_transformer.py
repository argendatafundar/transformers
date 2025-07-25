from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: pl.DataFrame, col: str, k: float) -> pl.DataFrame:
    return df.with_columns([
        (pl.col(col) * k).alias(col)
    ])

@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def concatenar_columnas(df: pl.DataFrame, cols: list, nueva_col: str, separtor: str = "-") -> pl.DataFrame:
    # Validate that all columns exist
    missing_cols = [col for col in cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    return df.with_columns([
        pl.concat_str(cols, separator=separtor).alias(nueva_col)
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

@transformer.convert
def cast_to(df: pl.DataFrame, col: str, target_type: str = "pl.Float64") -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).cast(eval(target_type), strict=False)
    ])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	df_sql(query="select * from self where region == 'Total'"),
	replace_value(col='semester', mapping={'I': '.0', 'II': '.49'}, alias=None),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor=''),
	cast_to(col='aniosem', target_type='pl.Float64'),
	drop_cols(cols='year'),
	drop_cols(cols='semester'),
	drop_cols(cols='region'),
	multiplicar_por_escalar(col='pov_rate', k=100),
	cast_to(col='k_value', target_type='pl.String'),
	replace_value(col='k_value', mapping={'0.25': 'k=0.25', '0.35': 'k=0.35'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where region == 'Total'")
#  
#  ------------------------------
#  
#  replace_value(col='semester', mapping={'I': '.0', 'II': '.49'}, alias=None)
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='')
#  
#  ------------------------------
#  
#  cast_to(col='aniosem', target_type='pl.Float64')
#  
#  ------------------------------
#  
#  drop_cols(cols='year')
#  
#  ------------------------------
#  
#  drop_cols(cols='semester')
#  
#  ------------------------------
#  
#  drop_cols(cols='region')
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='pov_rate', k=100)
#  
#  ------------------------------
#  
#  cast_to(col='k_value', target_type='pl.String')
#  
#  ------------------------------
#  
#  replace_value(col='k_value', mapping={'0.25': 'k=0.25', '0.35': 'k=0.35'}, alias=None)
#  
#  ------------------------------
#  