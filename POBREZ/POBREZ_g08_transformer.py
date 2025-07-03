from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def get_anios_mas_cercanos(df: pl.DataFrame, group_col: str, anio_col: str = 'anio', anio: int = 2021, thresh: int = 3) -> pl.DataFrame:
    """
    Get the years closest to a target year within a threshold, 
    but only for the maximum year within each group.

    Args:
        df: Polars DataFrame
        group_col: Column name to group by
        anio_col: Column name containing years (default: 'anio')
        anio: Target year (default: 2021)
        thresh: Threshold for year proximity (default: 3)

    Returns:
        Filtered DataFrame with years closest to target within threshold
    """
    return df.filter(
        # Is this the maximum year in the group?
        (pl.col(anio_col) == pl.col(anio_col).max().over(group_col)) &
        # Is this year within threshold of target year?
        ((pl.col(anio_col) - anio).abs() <= thresh)
    )

@transformer.convert
def multiplicar_por_escalar(df: pl.DataFrame, col: str, k: float) -> pl.DataFrame:
    return df.with_columns([
        (pl.col(col) * k).alias(col)
    ])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	df_sql(query='select * from self where poverty_line == 6.85'),
	drop_cols(cols='poverty_line'),
	multiplicar_por_escalar(col='poverty_rate', k=100),
	get_anios_mas_cercanos(group_col='geonombreFundar', anio_col='year', anio=2019, thresh=3)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  df_sql(query='select * from self where poverty_line == 6.85')
#  
#  ------------------------------
#  
#  drop_cols(cols='poverty_line')
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='poverty_rate', k=100)
#  
#  ------------------------------
#  
#  get_anios_mas_cercanos(group_col='geonombreFundar', anio_col='year', anio=2019, thresh=3)
#  
#  ------------------------------
#  