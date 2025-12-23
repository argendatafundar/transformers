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
#  RangeIndex: 7637 entries, 0 to 7636
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7637 non-null   int64  
#   1   importer_iso3    7637 non-null   object 
#   2   geonombreFundar  7593 non-null   object 
#   3   tipo_bien        7637 non-null   object 
#   4   expo             7637 non-null   float64
#   5   prop             7637 non-null   float64
#  
#  |    |   anio | importer_iso3   | geonombreFundar   | tipo_bien    |   expo |        prop |
#  |---:|-------:|:----------------|:------------------|:-------------|-------:|------------:|
#  |  0 |   2002 | AFG             | Afganistán        | Manufacturas |  84.33 | 0.000595445 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 314 entries, 3637 to 7636
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             314 non-null    int64  
#   1   importer_iso3    314 non-null    object 
#   2   geonombreFundar  312 non-null    object 
#   3   tipo_bien        314 non-null    object 
#   4   expo             314 non-null    float64
#   5   prop             314 non-null    float64
#  
#  |      |   anio | importer_iso3   | geonombreFundar   | tipo_bien   |    expo |      prop |
#  |-----:|-------:|:----------------|:------------------|:------------|--------:|----------:|
#  | 3637 |   2023 | ALB             | Albania           | Primarios   | 7655.34 | 0.0200102 |
#  
#  ------------------------------
#  