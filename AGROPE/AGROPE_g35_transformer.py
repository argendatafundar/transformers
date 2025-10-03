from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="cadena.isin(['Cítricos dulces', 'Tealera', 'Maní', 'Algodón textil', 'Yerba mate', 'Peras y Manzanas', 'Limón', 'Sojera', 'Vitivinícola', 'Tabacalera' , 'Fruta de carozo'])"),
	long_to_wide(index=['geonombreFundar'], columns='cadena', values='share')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 720 entries, 0 to 719
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  720 non-null    object 
#   1   geonombreFundar  720 non-null    object 
#   2   cadena           720 non-null    object 
#   3   share            720 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | cadena         |   share |
#  |---:|:------------------|:------------------|:---------------|--------:|
#  |  0 | AR-B              | Buenos Aires      | Algodón textil |   36.64 |
#  
#  ------------------------------
#  
#  query(condition="cadena.isin(['Cítricos dulces', 'Tealera', 'Maní', 'Algodón textil', 'Yerba mate', 'Peras y Manzanas', 'Limón', 'Sojera', 'Vitivinícola', 'Tabacalera' , 'Fruta de carozo'])")
#  Index: 264 entries, 0 to 717
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  264 non-null    object 
#   1   geonombreFundar  264 non-null    object 
#   2   cadena           264 non-null    object 
#   3   share            264 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | cadena         |   share |
#  |---:|:------------------|:------------------|:---------------|--------:|
#  |  0 | AR-B              | Buenos Aires      | Algodón textil |   36.64 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['geonombreFundar'], columns='cadena', values='share')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 12 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geonombreFundar   24 non-null     object 
#   1   Algodón textil    24 non-null     float64
#   2   Cítricos dulces   24 non-null     float64
#   3   Fruta de carozo   24 non-null     float64
#   4   Limón             24 non-null     float64
#   5   Maní              24 non-null     float64
#   6   Peras y Manzanas  24 non-null     float64
#   7   Sojera            24 non-null     float64
#   8   Tabacalera        24 non-null     float64
#   9   Tealera           24 non-null     float64
#   10  Vitivinícola      24 non-null     float64
#   11  Yerba mate        24 non-null     float64
#  
#  |    | geonombreFundar   |   Algodón textil |   Cítricos dulces |   Fruta de carozo |   Limón |   Maní |   Peras y Manzanas |   Sojera |   Tabacalera |   Tealera |   Vitivinícola |   Yerba mate |
#  |---:|:------------------|-----------------:|------------------:|------------------:|--------:|-------:|-------------------:|---------:|-------------:|----------:|---------------:|-------------:|
#  |  0 | Buenos Aires      |            36.64 |              5.75 |              8.29 |    0.52 |   4.86 |               0.65 |    16.35 |        30.13 |     17.37 |           0.96 |          0.3 |
#  
#  ------------------------------
#  