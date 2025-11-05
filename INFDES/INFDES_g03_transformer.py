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
	query(condition="cat_ocup_detalle!='Total formal'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 120 entries, 0 to 119
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        120 non-null    object 
#   1   geonombreFundar        120 non-null    object 
#   2   anio                   120 non-null    int64  
#   3   formal_def_productiva  120 non-null    object 
#   4   cat_ocup_cod           105 non-null    float64
#   5   cat_ocup_detalle       120 non-null    object 
#   6   valor                  120 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------|---------------:|:-------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Formal                  |              1 | Empleadores        |     3.7 |
#  
#  ------------------------------
#  
#  query(condition="cat_ocup_detalle!='Total formal'")
#  Index: 105 entries, 0 to 104
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        105 non-null    object 
#   1   geonombreFundar        105 non-null    object 
#   2   anio                   105 non-null    int64  
#   3   formal_def_productiva  105 non-null    object 
#   4   cat_ocup_cod           105 non-null    float64
#   5   cat_ocup_detalle       105 non-null    object 
#   6   valor                  105 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------|---------------:|:-------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Formal                  |              1 | Empleadores        |     3.7 |
#  
#  ------------------------------
#  