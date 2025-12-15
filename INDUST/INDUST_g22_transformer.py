from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def custom_logic(df: DataFrame) -> DataFrame:
    df['pais_seleccionado'] = df.apply(
        lambda row: 'Resto de Asia' if (row['geonombreFundar'] not in [
            'India', 'Turquía', 'China', 'Corea del Sur', 
            'Indonesia', 'Japón']) else row['geonombreFundar'], axis=1)
    return df

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	custom_logic(),
	agg_sum(key_cols=['anio', 'pais_seleccionado'], summarised_col='prop_industry_gdp')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2412 entries, 0 to 2411
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               2412 non-null   int64  
#   1   geocodigoFundar    2412 non-null   object 
#   2   geonombreFundar    2412 non-null   object 
#   3   industry_gdp       2412 non-null   float64
#   4   prop_industry_gdp  2412 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:------------------|:------------------|---------------:|--------------------:|
#  |  0 |   1970 | AFG               | Afganistán        |    1.04327e+09 |           0.0367871 |
#  
#  ------------------------------
#  
#  custom_logic()
#  RangeIndex: 2412 entries, 0 to 2411
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               2412 non-null   int64  
#   1   geocodigoFundar    2412 non-null   object 
#   2   geonombreFundar    2412 non-null   object 
#   3   industry_gdp       2412 non-null   float64
#   4   prop_industry_gdp  2412 non-null   float64
#   5   pais_seleccionado  2412 non-null   object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   industry_gdp |   prop_industry_gdp | pais_seleccionado   |
#  |---:|-------:|:------------------|:------------------|---------------:|--------------------:|:--------------------|
#  |  0 |   1970 | AFG               | Afganistán        |    1.04327e+09 |           0.0367871 | Resto de Asia       |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['anio', 'pais_seleccionado'], summarised_col='prop_industry_gdp')
#  RangeIndex: 378 entries, 0 to 377
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               378 non-null    int64  
#   1   pais_seleccionado  378 non-null    object 
#   2   prop_industry_gdp  378 non-null    float64
#  
#  |    |   anio | pais_seleccionado   |   prop_industry_gdp |
#  |---:|-------:|:--------------------|--------------------:|
#  |  0 |   1970 | China               |             1.03183 |
#  
#  ------------------------------
#  