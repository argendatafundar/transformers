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
	query(condition='anio <= 2022')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2527 entries, 0 to 2526
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigoFundar   2527 non-null   object 
#   1   geonombreFundar   2527 non-null   object 
#   2   anio              2527 non-null   int64  
#   3   gerd_gdp          2527 non-null   float64
#   4   nivel_agregacion  2527 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gerd_gdp | nivel_agregacion   |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------|
#  |  0 | AGO               | Angola            |   2016 |    0.03229 | pais               |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2022')
#  Index: 2500 entries, 0 to 2526
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigoFundar   2500 non-null   object 
#   1   geonombreFundar   2500 non-null   object 
#   2   anio              2500 non-null   int64  
#   3   gerd_gdp          2500 non-null   float64
#   4   nivel_agregacion  2500 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gerd_gdp | nivel_agregacion   |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------|
#  |  0 | AGO               | Angola            |   2016 |    0.03229 | pais               |
#  
#  ------------------------------
#  