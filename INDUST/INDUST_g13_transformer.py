from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024])'),
	multiplicar_por_escalar(col='prop', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 696 entries, 0 to 695
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          696 non-null    int64  
#   1   provincia_id  696 non-null    int64  
#   2   provincia     696 non-null    object 
#   3   prop          696 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |   prop |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   1913 |              2 | CABA        | 0.3719 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024])')
#  Index: 240 entries, 0 to 683
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          240 non-null    int64  
#   1   provincia_id  240 non-null    int64  
#   2   provincia     240 non-null    object 
#   3   prop          240 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |   prop |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   1913 |              2 | CABA        |  37.19 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  Index: 240 entries, 0 to 683
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          240 non-null    int64  
#   1   provincia_id  240 non-null    int64  
#   2   provincia     240 non-null    object 
#   3   prop          240 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |   prop |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   1913 |              2 | CABA        |  37.19 |
#  
#  ------------------------------
#  