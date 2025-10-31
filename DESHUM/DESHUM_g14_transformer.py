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
	query(condition="metrica == 'Ranking'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 616 entries, 0 to 615
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  616 non-null    object 
#   1   tipo_idh         616 non-null    object 
#   2   metrica          616 non-null    object 
#   3   valor            616 non-null    float64
#   4   geonombreFundar  616 non-null    object 
#  
#  |    | geocodigoFundar   | tipo_idh   | metrica   |   valor | geonombreFundar   |
#  |---:|:------------------|:-----------|:----------|--------:|:------------------|
#  |  0 | AFG               | IDH        | Índice    |   0.462 | Afganistán        |
#  
#  ------------------------------
#  
#  query(condition="metrica == 'Ranking'")
#  Index: 308 entries, 2 to 615
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  308 non-null    object 
#   1   tipo_idh         308 non-null    object 
#   2   metrica          308 non-null    object 
#   3   valor            308 non-null    float64
#   4   geonombreFundar  308 non-null    object 
#  
#  |    | geocodigoFundar   | tipo_idh   | metrica   |   valor | geonombreFundar   |
#  |---:|:------------------|:-----------|:----------|--------:|:------------------|
#  |  2 | AFG               | IDH        | Ranking   |     143 | Afganistán        |
#  
#  ------------------------------
#  