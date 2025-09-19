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
#  RangeIndex: 36089 entries, 0 to 36088
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             36089 non-null  int64  
#   1   geocodigoFundar  36089 non-null  object 
#   2   edad_mediana     36089 non-null  float64
#   3   geonombreFundar  36089 non-null  object 
#  
#  |    |   anio | geocodigoFundar   |   edad_mediana | geonombreFundar            |
#  |---:|-------:|:------------------|---------------:|:---------------------------|
#  |  0 |   1950 | LCN               |        18.3317 | América Latina y el Caribe |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 18164 entries, 0 to 36013
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             18164 non-null  int64  
#   1   geocodigoFundar  18164 non-null  object 
#   2   edad_mediana     18164 non-null  float64
#   3   geonombreFundar  18164 non-null  object 
#  
#  |    |   anio | geocodigoFundar   |   edad_mediana | geonombreFundar            |
#  |---:|-------:|:------------------|---------------:|:---------------------------|
#  |  0 |   1950 | LCN               |        18.3317 | América Latina y el Caribe |
#  
#  ------------------------------
#  