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
	query(condition="iso3 != 'MDV'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12059 entries, 0 to 12058
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                12059 non-null  int64  
#   1   iso3                12059 non-null  object 
#   2   pais                12059 non-null  object 
#   3   consumo_per_capita  11100 non-null  float64
#   4   nivel_agregacion    12059 non-null  object 
#  
#  |    |   anio | iso3   | pais       |   consumo_per_capita | nivel_agregacion   |
#  |---:|-------:|:-------|:-----------|---------------------:|:-------------------|
#  |  0 |   1961 | AFG    | Afganistán |            0.0514286 | pais               |
#  
#  ------------------------------
#  
#  query(condition="iso3 != 'MDV'")
#  Index: 11997 entries, 0 to 12058
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                11997 non-null  int64  
#   1   iso3                11997 non-null  object 
#   2   pais                11997 non-null  object 
#   3   consumo_per_capita  11038 non-null  float64
#   4   nivel_agregacion    11997 non-null  object 
#  
#  |    |   anio | iso3   | pais       |   consumo_per_capita | nivel_agregacion   |
#  |---:|-------:|:-------|:-----------|---------------------:|:-------------------|
#  |  0 |   1961 | AFG    | Afganistán |            0.0514286 | pais               |
#  
#  ------------------------------
#  