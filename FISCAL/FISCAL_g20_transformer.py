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
	query(condition='anio.isin([2010, 2014, 2018])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1598 entries, 0 to 1597
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1598 non-null   object 
#   1   anio             1598 non-null   int64  
#   2   valor            1598 non-null   float64
#   3   fuente           1598 non-null   object 
#   4   geonombreFundar  1598 non-null   object 
#  
#  |    | geocodigoFundar   |   anio |   valor | fuente   | geonombreFundar   |
#  |---:|:------------------|-------:|--------:|:---------|:------------------|
#  |  0 | ARG               |   2010 | 18.3908 | CEPAL    | Argentina         |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([2010, 2014, 2018])')
#  Index: 147 entries, 0 to 1594
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  147 non-null    object 
#   1   anio             147 non-null    int64  
#   2   valor            147 non-null    float64
#   3   fuente           147 non-null    object 
#   4   geonombreFundar  147 non-null    object 
#  
#  |    | geocodigoFundar   |   anio |   valor | fuente   | geonombreFundar   |
#  |---:|:------------------|-------:|--------:|:---------|:------------------|
#  |  0 | ARG               |   2010 | 18.3908 | CEPAL    | Argentina         |
#  
#  ------------------------------
#  