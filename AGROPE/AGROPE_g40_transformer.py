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
	query(condition='ranking <= 10')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1233 entries, 0 to 1232
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         1233 non-null   object 
#   1   pais_nombre  1223 non-null   object 
#   2   ncm6         1233 non-null   int64  
#   3   desc_ncm6    1233 non-null   object 
#   4   expo         1233 non-null   float64
#   5   share        1233 non-null   float64
#   6   ranking      1233 non-null   int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                           |    expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:----------------------------------------------------|--------:|--------:|----------:|
#  |  0 | AUT    | Austria       | 120190 | La soja, incluso quebrantadas, excepto para siembra | 78292.2 |   0.083 |        22 |
#  
#  ------------------------------
#  
#  query(condition='ranking <= 10')
#  Index: 100 entries, 6 to 1209
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         100 non-null    object 
#   1   pais_nombre  100 non-null    object 
#   2   ncm6         100 non-null    int64  
#   3   desc_ncm6    100 non-null    object 
#   4   expo         100 non-null    float64
#   5   share        100 non-null    float64
#   6   ranking      100 non-null    int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                       |   expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:------------------------------------------------|-------:|--------:|----------:|
#  |  6 | BOL    | Bolivia       | 150710 | Aceite de soja "soya" en bruto, incl. desgomado | 497561 |   4.816 |         4 |
#  
#  ------------------------------
#  