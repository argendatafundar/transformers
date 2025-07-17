from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def cast_to(df: pl.DataFrame, col: str, target_type: str = "pl.Float64") -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).cast(eval(target_type), strict=False)
    ])

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
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='semester', mapping={'I': 0, 'II': 5}, alias=None),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='.'),
	cast_to(col='aniosem', target_type='pl.Float64'),
	rename_cols(map={'hh_gender': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'}),
	replace_value(col='indicador', mapping={'Jefe_Varon': 'Jefe Varón', 'Jefa_Mujer': 'Jefa Mujer'}, alias=None),
	pl_filter(query="(pl.col('aniosem') == df.select(pl.col('aniosem').max()).item()) & (pl.col('indicador') != 'Total')")
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='semester', mapping={'I': 0, 'II': 5}, alias=None)
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='.')
#  
#  ------------------------------
#  
#  cast_to(col='aniosem', target_type='pl.Float64')
#  
#  ------------------------------
#  
#  rename_cols(map={'hh_gender': 'indicador', 'poverty_line': 'categoria', 'poverty_rate': 'valor'})
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'Jefe_Varon': 'Jefe Varón', 'Jefa_Mujer': 'Jefa Mujer'}, alias=None)
#  
#  ------------------------------
#  
#  pl_filter(query="(pl.col('aniosem') == df.select(pl.col('aniosem').max()).item()) & (pl.col('indicador') != 'Total')")
#  
#  ------------------------------
#  