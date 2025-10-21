from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10931 entries, 0 to 10930
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigoFundar       10931 non-null  object 
#   1   geonombreFundar       10931 non-null  object 
#   2   anio                  10931 non-null  int64  
#   3   gdp_indust_pc         10931 non-null  float64
#   4   gdp_indust_pc_indice  10931 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gdp_indust_pc |   gdp_indust_pc_indice |
#  |---:|:------------------|:------------------|-------:|----------------:|-----------------------:|
#  |  0 | ARG               | Argentina         |   1970 |         2877.23 |                    100 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 10931 entries, 0 to 10930
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigoFundar       10931 non-null  object 
#   1   geonombreFundar       10931 non-null  object 
#   2   anio                  10931 non-null  int64  
#   3   gdp_indust_pc         10931 non-null  float64
#   4   gdp_indust_pc_indice  10931 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   gdp_indust_pc |   gdp_indust_pc_indice |
#  |---:|:------------------|:------------------|-------:|----------------:|-----------------------:|
#  |  0 | ARG               | Argentina         |   1970 |         2877.23 |                    100 |
#  
#  ------------------------------
#  