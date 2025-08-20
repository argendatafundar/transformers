from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio >= 2010 and anio <= 2018')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 1615 entries, 0 to 1614
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1615 non-null   object 
#   1   anio             1615 non-null   int64  
#   2   valor            1615 non-null   float64
#   3   fuente           1615 non-null   object 
#   4   geonombreFundar  1615 non-null   object 
#  
#  |    | geocodigoFundar   |   anio |   valor | fuente   | geonombreFundar   |
#  |---:|:------------------|-------:|--------:|:---------|:------------------|
#  |  0 | ARG               |   2010 | 18.3908 | CEPAL    | Argentina         |
#  
#  ------------------------------
#  
#  query(condition='anio >= 2010 and anio <= 2018')
#  Index: 463 entries, 0 to 1611
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  463 non-null    object 
#   1   anio             463 non-null    int64  
#   2   valor            463 non-null    float64
#   3   fuente           463 non-null    object 
#   4   geonombreFundar  463 non-null    object 
#  
#  |    | geocodigoFundar   |   anio |   valor | fuente   | geonombreFundar   |
#  |---:|:------------------|-------:|--------:|:---------|:------------------|
#  |  0 | ARG               |   2010 | 18.3908 | CEPAL    | Argentina         |
#  
#  ------------------------------
#  