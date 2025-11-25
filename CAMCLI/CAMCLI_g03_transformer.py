from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: pl.DataFrame, by, descending = None):
    if not descending:
        descending = [False] * len(by)
    df = df.sort(by = by, descending= descending)
    return df

@transformer.convert
def pl_filter(df: pl.DataFrame, query: str):
    df = df.filter(eval(query))
    return df

@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(cols='valor_per_cap'),
	sort_values(by=['geocodigoFundar', 'anio'], descending=None),
	pl_filter(query='pl.col("anio") == df["anio"].max()')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_na(cols='valor_per_cap')
#  
#  ------------------------------
#  
#  sort_values(by=['geocodigoFundar', 'anio'], descending=None)
#  
#  ------------------------------
#  
#  pl_filter(query='pl.col("anio") == df["anio"].max()')
#  
#  ------------------------------
#  