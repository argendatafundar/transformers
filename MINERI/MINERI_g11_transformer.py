from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'provincia': 'geocodigo', 'vab_min_provincial': 'valor'}),
	replace_values(col='geocodigo', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No distribuído': 'MINERI_NO-DIST'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   provincia           475 non-null    object 
#   1   anio                475 non-null    int64  
#   2   vab_min_provincial  475 non-null    float64
#  
#  |    | provincia              |   anio |   vab_min_provincial |
#  |---:|:-----------------------|-------:|---------------------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 |              112.651 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo', 'vab_min_provincial': 'valor'})
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | geocodigo              |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_values(col='geocodigo', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No distribuído': 'MINERI_NO-DIST'})
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   2004 | 112.651 |
#  
#  ------------------------------
#  