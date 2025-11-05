from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="cat_ocup_detalle!='Total formal'"),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Chile', 'Uruguay', 'Costa Rica', 'Argentina', 'Brasil', 'México', 'Panamá', 'Rep. Dominicana', 'El Salvador', 'Colombia', 'Paraguay', 'Honduras', 'Perú', 'Ecuador', 'Bolivia'], col2='cat_ocup_cod', order2=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
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
#   1   geonombreFundar        105 non-null    category
#   2   anio                   105 non-null    int64   
#   3   formal_def_productiva  105 non-null    object  
#   4   cat_ocup_cod           105 non-null    category
#   5   cat_ocup_detalle       105 non-null    object  
#   6   valor                  105 non-null    float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------|---------------:|:-------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Formal                  |              1 | Empleadores        |     3.7 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Chile', 'Uruguay', 'Costa Rica', 'Argentina', 'Brasil', 'México', 'Panamá', 'Rep. Dominicana', 'El Salvador', 'Colombia', 'Paraguay', 'Honduras', 'Perú', 'Ecuador', 'Bolivia'], col2='cat_ocup_cod', order2=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
#  Index: 105 entries, 21 to 13
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype   
#  ---  ------                 --------------  -----   
#   0   geocodigoFundar        105 non-null    object  
#   1   geonombreFundar        105 non-null    category
#   2   anio                   105 non-null    int64   
#   3   formal_def_productiva  105 non-null    object  
#   4   cat_ocup_cod           105 non-null    category
#   5   cat_ocup_detalle       105 non-null    object  
#   6   valor                  105 non-null    float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | formal_def_productiva   |   cat_ocup_cod | cat_ocup_detalle   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------|---------------:|:-------------------|--------:|
#  | 21 | CHL               | Chile             |   2022 | Formal                  |              1 | Empleadores        | 4.08377 |
#  
#  ------------------------------
#  