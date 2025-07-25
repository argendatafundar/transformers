from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	df_sql(query="select * from self where poverty_line == 'Pobreza'"),
	replace_value(col='labor_status', mapping={'CtaPropia_no_Profesional': 'Cta. propia no prof.', 'CtaPropia_Profesional': 'Cta. propia prof.', 'Asalariado_Informal': 'Asal. informal', 'Asalariado_Formal': 'Asal. formal', 'Patrones': 'Patrón'}, alias=None),
	rename_cols(map={'labor_status': 'categoria', 'poverty_rate': 'valor'}),
	drop_cols(cols='year'),
	drop_cols(cols='semester'),
	drop_cols(cols='poverty_line'),
	df_sql(query="select * from self where (categoria != 'Ocupados') & (categoria != 'Desocupados') & (categoria != 'Inactivos') & (categoria != 'Sin_Salario') & (categoria != 'Total')")
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
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where poverty_line == 'Pobreza'")
#  
#  ------------------------------
#  
#  replace_value(col='labor_status', mapping={'CtaPropia_no_Profesional': 'Cta. propia no prof.', 'CtaPropia_Profesional': 'Cta. propia prof.', 'Asalariado_Informal': 'Asal. informal', 'Asalariado_Formal': 'Asal. formal', 'Patrones': 'Patrón'}, alias=None)
#  
#  ------------------------------
#  
#  rename_cols(map={'labor_status': 'categoria', 'poverty_rate': 'valor'})
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
#  drop_cols(cols='poverty_line')
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where (categoria != 'Ocupados') & (categoria != 'Desocupados') & (categoria != 'Inactivos') & (categoria != 'Sin_Salario') & (categoria != 'Total')")
#  
#  ------------------------------
#  