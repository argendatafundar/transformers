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
#  RangeIndex: 324 entries, 0 to 323
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               324 non-null    int64  
#   1   region             324 non-null    object 
#   2   industry_gdp       324 non-null    float64
#   3   prop_industry_gdp  324 non-null    float64
#  
#  |    |   anio | region         |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:---------------|---------------:|--------------------:|
#  |  0 |   1970 | América Latina |    2.42729e+11 |              8.5589 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 324 entries, 0 to 323
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               324 non-null    int64  
#   1   region             324 non-null    object 
#   2   industry_gdp       324 non-null    float64
#   3   prop_industry_gdp  324 non-null    float64
#  
#  |    |   anio | region         |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:---------------|---------------:|--------------------:|
#  |  0 |   1970 | América Latina |    2.42729e+11 |              8.5589 |
#  
#  ------------------------------
#  