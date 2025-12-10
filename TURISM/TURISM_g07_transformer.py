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
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 885 entries, 0 to 884
#  Data columns (total 4 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  885 non-null    object 
#   1   anio                       885 non-null    int64  
#   2   exportaciones_en_usd_mill  885 non-null    float64
#   3   tipo                       885 non-null    object 
#  
#  |    | categoria             |   anio |   exportaciones_en_usd_mill | tipo   |
#  |---:|:----------------------|-------:|----------------------------:|:-------|
#  |  0 | Complejos oleaginosos |   2006 |                        9770 | Bienes |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 50 entries, 553 to 884
#  Data columns (total 4 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#  
#  |     | categoria     |   anio |   exportaciones_en_usd_mill | tipo   |
#  |----:|:--------------|-------:|----------------------------:|:-------|
#  | 553 | Complejo soja |   2024 |                     19628.1 | Bienes |
#  
#  ------------------------------
#  