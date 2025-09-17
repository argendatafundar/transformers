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
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   8 non-null      int64  
#   1   tamanio_medio_hogares  8 non-null      float64
#  
#  |    |   anio |   tamanio_medio_hogares |
#  |---:|-------:|------------------------:|
#  |  0 |   1947 |                 4.55779 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   8 non-null      int64  
#   1   tamanio_medio_hogares  8 non-null      float64
#  
#  |    |   anio |   tamanio_medio_hogares |
#  |---:|-------:|------------------------:|
#  |  0 |   1947 |                 4.55779 |
#  
#  ------------------------------
#  