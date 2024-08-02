from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'}),
	drop_col(col=['letra', 'calificacion_cod'], axis=1),
	multiplicar_por_escalar(col='valor', k=100),
	ordenar_dos_columnas(col1='indicador', order1=['Calificado', 'Semicalificado', 'No calificado'], col2='categoria', order2=['Servicio doméstico', 'Agua y saneamiento', 'Transporte', 'Hotelería y restaurantes', 'Actividades administrativas', 'Comercio', 'Construcción', 'Industria manufacturera', 'Agro y pesca', 'Serv. comunitarios, sociales y personales', 'Petróleo y minería', 'Total', 'Electricidad y gas', 'Administración pública', 'Finanzas', 'Serv. inmobiliarios', 'Cultura', 'Salud', 'Información y comunicaciones', 'Enseñanza', 'Serv. profesionales', 'Organizaciones extraterritoriales']),
	drop_na(subset='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 514 entries, 0 to 513
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              514 non-null    int64  
#   1   letra             514 non-null    object 
#   2   letra_desc_abrev  514 non-null    object 
#   3   calificacion_cod  514 non-null    int64  
#   4   calificacion      514 non-null    object 
#   5   particip_calif    514 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|-------:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  0 |   2016 | N       | Actividades administrativas |                  1 | Calificado     |         0.113746 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 65 entries, 7 to 513
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             65 non-null     object 
#   1   letra_desc_abrev  65 non-null     object 
#   2   calificacion_cod  65 non-null     int64  
#   3   calificacion      65 non-null     object 
#   4   particip_calif    65 non-null     float64
#  
#  |    | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  7 | N       | Actividades administrativas |                  1 | Calificado     |         0.104882 |
#  
#  ------------------------------
#  
#  rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'})
#  Index: 65 entries, 7 to 513
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             65 non-null     object 
#   1   categoria         65 non-null     object 
#   2   calificacion_cod  65 non-null     int64  
#   3   indicador         65 non-null     object 
#   4   valor             65 non-null     float64
#  
#  |    | letra   | categoria                   |   calificacion_cod | indicador   |    valor |
#  |---:|:--------|:----------------------------|-------------------:|:------------|---------:|
#  |  7 | N       | Actividades administrativas |                  1 | Calificado  | 0.104882 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'calificacion_cod'], axis=1)
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  65 non-null     category
#   1   indicador  65 non-null     category
#   2   valor      65 non-null     float64 
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  65 non-null     category
#   1   indicador  65 non-null     category
#   2   valor      65 non-null     float64 
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='indicador', order1=['Calificado', 'Semicalificado', 'No calificado'], col2='categoria', order2=['Servicio doméstico', 'Agua y saneamiento', 'Transporte', 'Hotelería y restaurantes', 'Actividades administrativas', 'Comercio', 'Construcción', 'Industria manufacturera', 'Agro y pesca', 'Serv. comunitarios, sociales y personales', 'Petróleo y minería', 'Total', 'Electricidad y gas', 'Administración pública', 'Finanzas', 'Serv. inmobiliarios', 'Cultura', 'Salud', 'Información y comunicaciones', 'Enseñanza', 'Serv. profesionales', 'Organizaciones extraterritoriales'])
#  Index: 65 entries, 449 to 433
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  65 non-null     category
#   1   indicador  65 non-null     category
#   2   valor      65 non-null     float64 
#  
#  |     | categoria          | indicador   |    valor |
#  |----:|:-------------------|:------------|---------:|
#  | 449 | Servicio doméstico | Calificado  | 0.774139 |
#  
#  ------------------------------
#  
#  drop_na(subset='valor')
#  Index: 65 entries, 449 to 433
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  65 non-null     category
#   1   indicador  65 non-null     category
#   2   valor      65 non-null     float64 
#  
#  |     | categoria          | indicador   |    valor |
#  |----:|:-------------------|:------------|---------:|
#  | 449 | Servicio doméstico | Calificado  | 0.774139 |
#  
#  ------------------------------
#  