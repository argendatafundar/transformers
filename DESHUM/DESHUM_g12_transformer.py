from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	rename_cols(map={'sexo': 'indicador', 'idh': 'valor'}),
	query(condition="pais_nombre in ('Argentina', 'Yemen','Afganistán','Iraq','Siria','Mundo', 'Somalia', 'Burundi', 'India', 'Jordania', 'Suecia', 'América Latina y el Caribe', 'Noruega', 'Suiza', 'España', 'Italia', 'Chile', 'Uruguay', 'Liechtenstein')")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10028 entries, 0 to 10027
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         10028 non-null  object 
#   1   anio         10028 non-null  int64  
#   2   pais_nombre  10028 non-null  object 
#   3   sexo         10028 non-null  object 
#   4   idh          10028 non-null  float64
#  
#  |    | iso3   |   anio | pais_nombre   | sexo    |   idh |
#  |---:|:-------|-------:|:--------------|:--------|------:|
#  |  0 | AFG    |   2008 | Afganistán    | Varones | 0.499 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 386 entries, 28 to 10027
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         386 non-null    object 
#   1   anio         386 non-null    int64  
#   2   pais_nombre  386 non-null    object 
#   3   sexo         386 non-null    object 
#   4   idh          386 non-null    float64
#  
#  |    | iso3   |   anio | pais_nombre   | sexo    |   idh |
#  |---:|:-------|-------:|:--------------|:--------|------:|
#  | 28 | AFG    |   2022 | Afganistán    | Varones | 0.534 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'indicador', 'idh': 'valor'})
#  Index: 386 entries, 28 to 10027
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         386 non-null    object 
#   1   anio         386 non-null    int64  
#   2   pais_nombre  386 non-null    object 
#   3   indicador    386 non-null    object 
#   4   valor        386 non-null    float64
#  
#  |    | iso3   |   anio | pais_nombre   | indicador   |   valor |
#  |---:|:-------|-------:|:--------------|:------------|--------:|
#  | 28 | AFG    |   2022 | Afganistán    | Varones     |   0.534 |
#  
#  ------------------------------
#  
#  query(condition="pais_nombre in ('Argentina', 'Yemen','Afganistán','Iraq','Siria','Mundo', 'Somalia', 'Burundi', 'India', 'Jordania', 'Suecia', 'América Latina y el Caribe', 'Noruega', 'Suiza', 'España', 'Italia', 'Chile', 'Uruguay', 'Liechtenstein')")
#  Index: 38 entries, 28 to 10027
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         38 non-null     object 
#   1   anio         38 non-null     int64  
#   2   pais_nombre  38 non-null     object 
#   3   indicador    38 non-null     object 
#   4   valor        38 non-null     float64
#  
#  |    | iso3   |   anio | pais_nombre   | indicador   |   valor |
#  |---:|:-------|-------:|:--------------|:------------|--------:|
#  | 28 | AFG    |   2022 | Afganistán    | Varones     |   0.534 |
#  
#  ------------------------------
#  