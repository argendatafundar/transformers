from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([2004,2012,2023])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 240 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       240 non-null    int64  
#   1   vab_pb     240 non-null    float64
#   2   provincia  240 non-null    object 
#   3   share      240 non-null    float64
#   4   geocodigo  240 non-null    object 
#  
#  |    |   anio |   vab_pb | provincia   |   share | geocodigo   |
#  |---:|-------:|---------:|:------------|--------:|:------------|
#  |  0 |   2004 |  37.1713 | CABA        | 2.57688 | AR-C        |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([2004,2012,2023])')
#  Index: 36 entries, 0 to 239
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       36 non-null     int64  
#   1   vab_pb     36 non-null     float64
#   2   provincia  36 non-null     object 
#   3   share      36 non-null     float64
#   4   geocodigo  36 non-null     object 
#  
#  |    |   anio |   vab_pb | provincia   |   share | geocodigo   |
#  |---:|-------:|---------:|:------------|--------:|:------------|
#  |  0 |   2004 |  37.1713 | CABA        | 2.57688 | AR-C        |
#  
#  ------------------------------
#  