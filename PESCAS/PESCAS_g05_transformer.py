from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar Illex |        372.706 |    0.208227 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar Illex |        372.706 |    0.208227 |
#  
#  ------------------------------
#  