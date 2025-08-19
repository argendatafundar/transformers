from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame, dummy = True) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity(dummy=True)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  identity(dummy=True)
#  
#  ------------------------------
#  