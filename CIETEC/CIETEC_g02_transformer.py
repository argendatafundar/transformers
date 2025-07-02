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
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype 
#  ---  ------                     --------------  ----- 
#   0   anio                       3 non-null      int64 
#   1   sector_de_financiamiento   3 non-null      object
#   2   x_de_inversion_por_sector  3 non-null      int64 
#  
#  |    |   anio | sector_de_financiamiento   |   x_de_inversion_por_sector |
#  |---:|-------:|:---------------------------|----------------------------:|
#  |  0 |   2023 | Sector Público             |                          55 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype 
#  ---  ------                     --------------  ----- 
#   0   anio                       3 non-null      int64 
#   1   sector_de_financiamiento   3 non-null      object
#   2   x_de_inversion_por_sector  3 non-null      int64 
#  
#  |    |   anio | sector_de_financiamiento   |   x_de_inversion_por_sector |
#  |---:|-------:|:---------------------------|----------------------------:|
#  |  0 |   2023 | Sector Público             |                          55 |
#  
#  ------------------------------
#  