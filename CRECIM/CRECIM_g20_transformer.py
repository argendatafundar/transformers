from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def agregacion_suma(df:DataFrame, group_cols:list[str], col_sum:str):
    df_gr = df.groupby(group_cols)[col_sum].sum().reset_index()
    return df_gr

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
agregacion_suma(group_cols=['region_pbg', 'anio'], col_sum='participacion_vab'),
	rename_cols(map={'region_pbg': 'indicador', 'participacion_vab': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	query(condition='anio.isin([1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2022])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               672 non-null    int64  
#   1   region_pbg         672 non-null    object 
#   2   participacion_vab  672 non-null    float64
#   3   vab                672 non-null    float64
#   4   provincia_id       672 non-null    object 
#  
#  |    |   anio | region_pbg      |   participacion_vab |     vab | provincia_id   |
#  |---:|-------:|:----------------|--------------------:|--------:|:---------------|
#  |  0 |   1895 | Pampeana y AMBA |             0.23917 | 4158.05 | AR-B           |
#  
#  ------------------------------
#  
#  agregacion_suma(group_cols=['region_pbg', 'anio'], col_sum='participacion_vab')
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   region_pbg         140 non-null    object 
#   1   anio               140 non-null    int64  
#   2   participacion_vab  140 non-null    float64
#  
#  |    | region_pbg   |   anio |   participacion_vab |
#  |---:|:-------------|-------:|--------------------:|
#  |  0 | Cuyo         |   1895 |           0.0719181 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region_pbg': 'indicador', 'participacion_vab': 'valor'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  140 non-null    object 
#   1   anio       140 non-null    int64  
#   2   valor      140 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cuyo        |   1895 | 7.19181 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  140 non-null    object 
#   1   anio       140 non-null    int64  
#   2   valor      140 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cuyo        |   1895 | 7.19181 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2022])')
#  Index: 55 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  55 non-null     object 
#   1   anio       55 non-null     int64  
#   2   valor      55 non-null     float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cuyo        |   1895 | 7.19181 |
#  
#  ------------------------------
#  