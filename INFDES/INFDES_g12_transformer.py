from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="sexo == 'Total'"),
	multiplicar_por_escalar(col='tasa_desempleo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21483 entries, 0 to 21482
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    21483 non-null  object 
#   1   geonombreFundar    21483 non-null  object 
#   2   anio               21483 non-null  int64  
#   3   sexo               21483 non-null  object 
#   4   tasa_desempleo     18504 non-null  float64
#   5   continente_fundar  21483 non-null  object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_desempleo | continente_fundar   |
#  |---:|:------------------|:------------------|-------:|:-------|-----------------:|:--------------------|
#  |  0 | AFG               | Afganistán        |   2023 | Total  |          0.14386 | Asia                |
#  
#  ------------------------------
#  
#  query(condition="sexo == 'Total'")
#  Index: 7161 entries, 0 to 21480
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    7161 non-null   object 
#   1   geonombreFundar    7161 non-null   object 
#   2   anio               7161 non-null   int64  
#   3   sexo               7161 non-null   object 
#   4   tasa_desempleo     6168 non-null   float64
#   5   continente_fundar  7161 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_desempleo | continente_fundar   |
#  |---:|:------------------|:------------------|-------:|:-------|-----------------:|:--------------------|
#  |  0 | AFG               | Afganistán        |   2023 | Total  |           14.386 | Asia                |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_desempleo', k=100)
#  Index: 7161 entries, 0 to 21480
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    7161 non-null   object 
#   1   geonombreFundar    7161 non-null   object 
#   2   anio               7161 non-null   int64  
#   3   sexo               7161 non-null   object 
#   4   tasa_desempleo     6168 non-null   float64
#   5   continente_fundar  7161 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | sexo   |   tasa_desempleo | continente_fundar   |
#  |---:|:------------------|:------------------|-------:|:-------|-----------------:|:--------------------|
#  |  0 | AFG               | Afganistán        |   2023 | Total  |           14.386 | Asia                |
#  
#  ------------------------------
#  