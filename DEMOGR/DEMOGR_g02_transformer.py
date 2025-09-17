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
	query(condition='anio <= 2025')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 156 entries, 0 to 155
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          156 non-null    int64  
#   1   edad_mediana  155 non-null    float64
#   2   fuente        156 non-null    object 
#  
#  |    |   anio |   edad_mediana | fuente   |
#  |---:|-------:|---------------:|:---------|
#  |  0 |   1869 |        16.5049 | INDEC    |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          80 non-null     int64  
#   1   edad_mediana  80 non-null     float64
#   2   fuente        80 non-null     object 
#  
#  |    |   anio |   edad_mediana | fuente   |
#  |---:|-------:|---------------:|:---------|
#  |  0 |   1869 |        16.5049 | INDEC    |
#  
#  ------------------------------
#  