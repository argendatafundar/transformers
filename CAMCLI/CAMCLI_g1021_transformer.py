from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: pl.DataFrame, col: str, k: float) -> pl.DataFrame:
    return df.with_columns([
        (pl.col(col) * k).alias(col)
    ])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  
#  ------------------------------
#  