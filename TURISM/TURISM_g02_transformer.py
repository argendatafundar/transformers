from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def filtrar_por_max_anio(df, group_cols):
    idx = df.groupby(group_cols)['anio'].transform('max') == df['anio']
    return df[idx]

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio not in [2020, 2021]'),
	filtrar_por_max_anio(group_cols=['geocodigoFundar', 'geonombreFundar'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1243 entries, 0 to 1242
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               1243 non-null   int64  
#   1   geocodigoFundar    1243 non-null   object 
#   2   geonombreFundar    1243 non-null   object 
#   3   share_tourism_gdp  1243 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_tourism_gdp |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   2008 | ALB               | Albania           |             2.75707 |
#  
#  ------------------------------
#  
#  query(condition='anio not in [2020, 2021]')
#  Index: 1085 entries, 0 to 1242
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               1085 non-null   int64  
#   1   geocodigoFundar    1085 non-null   object 
#   2   geonombreFundar    1085 non-null   object 
#   3   share_tourism_gdp  1085 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_tourism_gdp |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  |  0 |   2008 | ALB               | Albania           |             2.75707 |
#  
#  ------------------------------
#  
#  filtrar_por_max_anio(group_cols=['geocodigoFundar', 'geonombreFundar'])
#  Index: 125 entries, 15 to 1242
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               125 non-null    int64  
#   1   geocodigoFundar    125 non-null    object 
#   2   geonombreFundar    125 non-null    object 
#   3   share_tourism_gdp  125 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_tourism_gdp |
#  |---:|-------:|:------------------|:------------------|--------------------:|
#  | 15 |   2023 | ALB               | Albania           |             5.56238 |
#  
#  ------------------------------
#  