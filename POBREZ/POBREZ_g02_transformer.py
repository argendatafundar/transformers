from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	df_sql(query="select * from self where year  in (1980, 2022) and province != 'argentina'")
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where year  in (1980, 2022) and province != 'argentina'")
#  
#  ------------------------------
#  