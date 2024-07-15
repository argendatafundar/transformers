from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col=['anio'], axis=1),
	rename_cols(map={'vab_min_vab_total_prov': 'valor', 'provincia': 'geocodigo'}),
	replace_values(col='geocodigo', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No_distribuido': 'MINERI_NO-DIST'}),
	query(condition='geocodigo != "MINERI_NO-DIST"')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   provincia               475 non-null    object 
#   1   anio                    475 non-null    int64  
#   2   vab_min_vab_total_prov  475 non-null    float64
#  
#  |    | provincia    |   anio |   vab_min_vab_total_prov |
#  |---:|:-------------|-------:|-------------------------:|
#  |  0 | Buenos_Aires |   2004 |                 0.212523 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 25 entries, 18 to 474
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   provincia               25 non-null     object 
#   1   anio                    25 non-null     int64  
#   2   vab_min_vab_total_prov  25 non-null     float64
#  
#  |    | provincia    |   anio |   vab_min_vab_total_prov |
#  |---:|:-------------|-------:|-------------------------:|
#  | 18 | Buenos_Aires |   2022 |                 0.259416 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio'], axis=1)
#  Index: 25 entries, 18 to 474
#  Data columns (total 2 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   provincia               25 non-null     object 
#   1   vab_min_vab_total_prov  25 non-null     float64
#  
#  |    | provincia    |   vab_min_vab_total_prov |
#  |---:|:-------------|-------------------------:|
#  | 18 | Buenos_Aires |                 0.259416 |
#  
#  ------------------------------
#  
#  rename_cols(map={'vab_min_vab_total_prov': 'valor', 'provincia': 'geocodigo'})
#  Index: 25 entries, 18 to 474
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | geocodigo    |    valor |
#  |---:|:-------------|---------:|
#  | 18 | Buenos_Aires | 0.259416 |
#  
#  ------------------------------
#  
#  replace_values(col='geocodigo', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No_distribuido': 'MINERI_NO-DIST'})
#  Index: 25 entries, 18 to 474
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |    | geocodigo   |    valor |
#  |---:|:------------|---------:|
#  | 18 | AR-B        | 0.259416 |
#  
#  ------------------------------
#  
#  query(condition='geocodigo != "MINERI_NO-DIST"')
#  Index: 24 entries, 18 to 474
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  24 non-null     object 
#   1   valor      24 non-null     float64
#  
#  |    | geocodigo   |    valor |
#  |---:|:------------|---------:|
#  | 18 | AR-B        | 0.259416 |
#  
#  ------------------------------
#  