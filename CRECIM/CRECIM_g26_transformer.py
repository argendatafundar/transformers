from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'pib_per_capita'], axis=1)
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
#  drop_col(col=['geocodigoFundar', 'pib_per_capita'], axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  7831 non-null   object 
#   1   anio             7831 non-null   int64  
#   2   cambio_relativo  7831 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   cambio_relativo |
#  |---:|:------------------|-------:|------------------:|
#  |  0 | Argentina         |   1820 |                 0 |
#  
#  ------------------------------
#  