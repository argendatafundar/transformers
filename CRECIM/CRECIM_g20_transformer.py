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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
agregacion_suma(group_cols=['region_pbg', 'anio'], col_sum='participacion_vab'),
	rename_cols(map={'region_pbg': 'indicador', 'participacion_vab': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   provincia_id       672 non-null    int64  
#   1   provincia_nombre   672 non-null    object 
#   2   region_pbg         672 non-null    object 
#   3   anio               672 non-null    int64  
#   4   vab                672 non-null    float64
#   5   participacion_vab  672 non-null    float64
#  
#  |    |   provincia_id | provincia_nombre                | region_pbg      |   anio |    vab |   participacion_vab |
#  |---:|---------------:|:--------------------------------|:----------------|-------:|-------:|--------------------:|
#  |  0 |              2 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   1895 | 3899.1 |              0.2243 |
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
#  |  0 | Cuyo         |   1895 |               0.072 |
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
#  |  0 | Cuyo        |   1895 |     7.2 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
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
#  |  0 | Cuyo        |   1895 |     7.2 |
#  
#  ------------------------------
#  