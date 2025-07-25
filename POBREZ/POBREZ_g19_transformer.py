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

@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(cols=['year', 'poverty_rate']),
	replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	df_sql(query="select * from self where poverty_line == 'Pobreza'"),
	replace_value(col='age_group', mapping={'0_14_years': '0-14', '15_29_years': '15-29', '30_64_years': '30-64', '65_and_more_years': '65 y más'}, alias=None),
	rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_na(cols=['year', 'poverty_rate'])
#  
#  ------------------------------
#  
#  replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None)
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where poverty_line == 'Pobreza'")
#  
#  ------------------------------
#  
#  replace_value(col='age_group', mapping={'0_14_years': '0-14', '15_29_years': '15-29', '30_64_years': '30-64', '65_and_more_years': '65 y más'}, alias=None)
#  
#  ------------------------------
#  
#  rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'})
#  
#  ------------------------------
#  