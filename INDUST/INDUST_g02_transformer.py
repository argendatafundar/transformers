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
#  RangeIndex: 294 entries, 0 to 293
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         294 non-null    int64  
#   1   letra        294 non-null    object 
#   2   letra_desc   294 non-null    object 
#   3   prop_empleo  294 non-null    float64
#   4   prop_vab     294 non-null    float64
#  
#  |    |   anio | letra   | letra_desc   |   prop_empleo |   prop_vab |
#  |---:|-------:|:--------|:-------------|--------------:|-----------:|
#  |  0 |   2004 | A_B     | Agro y pesca |     0.0836072 |  0.0983632 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 14 entries, 280 to 293
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         14 non-null     int64  
#   1   letra        14 non-null     object 
#   2   letra_desc   14 non-null     object 
#   3   prop_empleo  14 non-null     float64
#   4   prop_vab     14 non-null     float64
#  
#  |     |   anio | letra   | letra_desc   |   prop_empleo |   prop_vab |
#  |----:|-------:|:--------|:-------------|--------------:|-----------:|
#  | 280 |   2024 | A_B     | Agro y pesca |     0.0644079 |  0.0717834 |
#  
#  ------------------------------
#  