from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   rama_etq  6 non-null      object 
#   1   emp       6 non-null      int64  
#   2   prop      6 non-null      float64
#  
#  |    | rama_etq    |    emp |    prop |
#  |---:|:------------|-------:|--------:|
#  |  0 | Gastronomía | 871409 | 44.5475 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   rama_etq  6 non-null      object 
#   1   emp       6 non-null      int64  
#   2   prop      6 non-null      float64
#  
#  |    | rama_etq    |    emp |    prop |
#  |---:|:------------|-------:|--------:|
#  |  0 | Gastronomía | 871409 | 44.5475 |
#  
#  ------------------------------
#  