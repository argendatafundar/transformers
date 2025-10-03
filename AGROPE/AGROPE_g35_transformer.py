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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
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
#  long_to_wide(index=['geonombreFundar'], columns='cadena', values='share')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 31 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geonombreFundar   24 non-null     object 
#   1   Algodón textil    24 non-null     float64
#   2   Apícola           24 non-null     float64
#   3   Arrocera          24 non-null     float64
#   4   Avícola           24 non-null     float64
#   5   Azucarera         24 non-null     float64
#   6   Bovina            24 non-null     float64
#   7   Caprina           24 non-null     float64
#   8   Cebada            24 non-null     float64
#   9   Cítricos dulces   24 non-null     float64
#   10  Fruta de carozo   24 non-null     float64
#   11  Frutas finas      24 non-null     float64
#   12  Girasol           24 non-null     float64
#   13  Hortícola         24 non-null     float64
#   14  Legumbres         24 non-null     float64
#   15  Limón             24 non-null     float64
#   16  Láctea            24 non-null     float64
#   17  Maicera           24 non-null     float64
#   18  Maní              24 non-null     float64
#   19  Olivícola         24 non-null     float64
#   20  Ovina             24 non-null     float64
#   21  Peras y Manzanas  24 non-null     float64
#   22  Pesca             24 non-null     float64
#   23  Porcina           24 non-null     float64
#   24  Sojera            24 non-null     float64
#   25  Sorgo             24 non-null     float64
#   26  Tabacalera        24 non-null     float64
#   27  Tealera           24 non-null     float64
#   28  Triguera          24 non-null     float64
#   29  Vitivinícola      24 non-null     float64
#   30  Yerba mate        24 non-null     float64
#  
#  |    | geonombreFundar   |   Algodón textil |   Apícola |   Arrocera |   Avícola |   Azucarera |   Bovina |   Caprina |   Cebada |   Cítricos dulces |   Fruta de carozo |   Frutas finas |   Girasol |   Hortícola |   Legumbres |   Limón |   Láctea |   Maicera |   Maní |   Olivícola |   Ovina |   Peras y Manzanas |   Pesca |   Porcina |   Sojera |   Sorgo |   Tabacalera |   Tealera |   Triguera |   Vitivinícola |   Yerba mate |
#  |---:|:------------------|-----------------:|----------:|-----------:|----------:|------------:|---------:|----------:|---------:|------------------:|------------------:|---------------:|----------:|------------:|------------:|--------:|---------:|----------:|-------:|------------:|--------:|-------------------:|--------:|----------:|---------:|--------:|-------------:|----------:|-----------:|---------------:|-------------:|
#  |  0 | Buenos Aires      |            36.64 |     35.38 |       0.17 |     29.68 |          18 |    40.83 |      0.63 |    54.24 |              5.75 |              8.29 |           4.47 |     47.51 |       19.23 |        5.49 |    0.52 |    31.52 |     27.27 |   4.86 |        2.55 |   10.95 |               0.65 |   46.48 |     39.12 |    16.35 |   38.45 |        30.13 |     17.37 |      40.92 |           0.96 |          0.3 |
#  
#  ------------------------------
#  