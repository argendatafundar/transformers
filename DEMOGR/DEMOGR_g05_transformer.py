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
	query(condition='anio <= 2025 and anio >= 1950')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 156 entries, 0 to 155
#  Data columns (total 2 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              156 non-null    int64  
#   1   tasa_dependencia  156 non-null    float64
#  
#  |    |   anio |   tasa_dependencia |
#  |---:|-------:|-------------------:|
#  |  0 |   1869 |             123.34 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025 and anio >= 1950')
#  Index: 76 entries, 4 to 79
#  Data columns (total 2 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              76 non-null     int64  
#   1   tasa_dependencia  76 non-null     float64
#  
#  |    |   anio |   tasa_dependencia |
#  |---:|-------:|-------------------:|
#  |  4 |   1950 |            79.7666 |
#  
#  ------------------------------
#  