from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    return df.rename(map)

@transformer.convert
def pl_filter(df: pl.DataFrame, query: str):
    df = df.filter(eval(query))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pl_filter(query="pl.col('province') == 'argentina'"),
	drop_cols(cols='province'),
	rename_cols(map={'year': 'categoria', 'nbi_rate': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  pl_filter(query="pl.col('province') == 'argentina'")
#  
#  ------------------------------
#  
#  drop_cols(cols='province')
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'categoria', 'nbi_rate': 'valor'})
#  
#  ------------------------------
#  