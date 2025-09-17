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
#  RangeIndex: 35787 entries, 0 to 35786
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             35787 non-null  int64  
#   1   geocodigoFundar  35787 non-null  object 
#   2   edad_mediana     35787 non-null  float64
#   3   geonombreFundar  35787 non-null  object 
#  
#  |    |   anio | geocodigoFundar   |   edad_mediana | geonombreFundar   |
#  |---:|-------:|:------------------|---------------:|:------------------|
#  |  0 |   1950 | BDI               |        18.3054 | Burundi           |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 18012 entries, 0 to 35711
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             18012 non-null  int64  
#   1   geocodigoFundar  18012 non-null  object 
#   2   edad_mediana     18012 non-null  float64
#   3   geonombreFundar  18012 non-null  object 
#  
#  |    |   anio | geocodigoFundar   |   edad_mediana | geonombreFundar   |
#  |---:|-------:|:------------------|---------------:|:------------------|
#  |  0 |   1950 | BDI               |        18.3054 | Burundi           |
#  
#  ------------------------------
#  