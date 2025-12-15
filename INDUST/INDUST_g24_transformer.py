from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='geonombreFundar', curr_value='Promedio ponderado Asia', new_value='Promedio Asia')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2520 entries, 0 to 2519
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    2520 non-null   int64  
#   1   geocodigoFundar         2466 non-null   object 
#   2   geonombreFundar         2520 non-null   object 
#   3   indust_gdp_pc           2520 non-null   float64
#   4   indust_gdp_pc_relative  2520 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   indust_gdp_pc |   indust_gdp_pc_relative |
#  |---:|-------:|:------------------|:------------------|----------------:|-------------------------:|
#  |  0 |   1970 | AFG               | Afganistán        |         92.4059 |                  2.46116 |
#  
#  ------------------------------
#  
#  replace_value(col='geonombreFundar', curr_value='Promedio ponderado Asia', new_value='Promedio Asia')
#  RangeIndex: 2520 entries, 0 to 2519
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    2520 non-null   int64  
#   1   geocodigoFundar         2466 non-null   object 
#   2   geonombreFundar         2520 non-null   object 
#   3   indust_gdp_pc           2520 non-null   float64
#   4   indust_gdp_pc_relative  2520 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   indust_gdp_pc |   indust_gdp_pc_relative |
#  |---:|-------:|:------------------|:------------------|----------------:|-------------------------:|
#  |  0 |   1970 | AFG               | Afganistán        |         92.4059 |                  2.46116 |
#  
#  ------------------------------
#  