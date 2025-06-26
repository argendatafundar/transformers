from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
	query(condition='anio <= 1900'),
	drop_col(col=['geocodigoFundar', 'pib_per_capita'], axis=1),
	multiplicar_por_escalar(col='cambio_relativo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  7831 non-null   object 
#   1   geonombreFundar  7831 non-null   object 
#   2   anio             7831 non-null   int64  
#   3   pib_per_capita   7831 non-null   float64
#   4   cambio_relativo  7831 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:------------------|:------------------|-------:|-----------------:|------------------:|
#  |  0 | ARG               | Argentina         |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 1900')
#  Index: 1949 entries, 0 to 7666
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1949 non-null   object 
#   1   geonombreFundar  1949 non-null   object 
#   2   anio             1949 non-null   int64  
#   3   pib_per_capita   1949 non-null   float64
#   4   cambio_relativo  1949 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:------------------|:------------------|-------:|-----------------:|------------------:|
#  |  0 | ARG               | Argentina         |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'pib_per_capita'], axis=1)
#  Index: 1949 entries, 0 to 7666
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  1949 non-null   object 
#   1   anio             1949 non-null   int64  
#   2   cambio_relativo  1949 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   cambio_relativo |
#  |---:|:------------------|-------:|------------------:|
#  |  0 | Argentina         |   1820 |                 0 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='cambio_relativo', k=100)
#  Index: 1949 entries, 0 to 7666
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  1949 non-null   object 
#   1   anio             1949 non-null   int64  
#   2   cambio_relativo  1949 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   cambio_relativo |
#  |---:|:------------------|-------:|------------------:|
#  |  0 | Argentina         |   1820 |                 0 |
#  
#  ------------------------------
#  