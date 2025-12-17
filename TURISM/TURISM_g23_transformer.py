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
	query(condition='anio >= 1995')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 245 entries, 0 to 244
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             245 non-null    int64  
#   1   region           245 non-null    object 
#   2   expo_turisticas  245 non-null    float64
#  
#  |    |   anio | region   |   expo_turisticas |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1976 | Américas |           9.11311 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1995')
#  Index: 150 entries, 95 to 244
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             150 non-null    int64  
#   1   region           150 non-null    object 
#   2   expo_turisticas  150 non-null    float64
#  
#  |    |   anio | region   |   expo_turisticas |
#  |---:|-------:|:---------|------------------:|
#  | 95 |   1995 | Américas |           103.625 |
#  
#  ------------------------------
#  