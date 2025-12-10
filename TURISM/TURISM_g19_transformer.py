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
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                13 non-null     int64  
#   1   proporcion_viajera  13 non-null     float64
#  
#  |    |   anio |   proporcion_viajera |
#  |---:|-------:|---------------------:|
#  |  0 |   2010 |              43.4914 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                13 non-null     int64  
#   1   proporcion_viajera  13 non-null     float64
#  
#  |    |   anio |   proporcion_viajera |
#  |---:|-------:|---------------------:|
#  |  0 |   2010 |              43.4914 |
#  
#  ------------------------------
#  