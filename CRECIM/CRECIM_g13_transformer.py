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
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'}),
	query(condition="geocodigo not in ['LAC','TLA','SSA','TSS', 'EAP','ECA','MNA','TSA','TSS', 'TEC','TEA']")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8083 entries, 0 to 8082
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    8083 non-null   object 
#   1   anio    8083 non-null   int64  
#   2   pib_pc  8083 non-null   float64
#  
#  |    | iso3   |   anio |   pib_pc |
#  |---:|:-------|-------:|---------:|
#  |  0 | AFE    |   2023 |  3967.86 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 236 entries, 0 to 8049
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    236 non-null    object 
#   1   pib_pc  236 non-null    float64
#  
#  |    | iso3   |   pib_pc |
#  |---:|:-------|---------:|
#  |  0 | AFE    |  3967.86 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'})
#  Index: 236 entries, 0 to 8049
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  236 non-null    object 
#   1   valor      236 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AFE         | 3967.86 |
#  
#  ------------------------------
#  
#  query(condition="geocodigo not in ['LAC','TLA','SSA','TSS', 'EAP','ECA','MNA','TSA','TSS', 'TEC','TEA']")
#  Index: 226 entries, 0 to 8049
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  226 non-null    object 
#   1   valor      226 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AFE         | 3967.86 |
#  
#  ------------------------------
#  