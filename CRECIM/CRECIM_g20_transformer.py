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
def completar_datos_faltantes(df, col_anio, cols_cat, col_valor, anio_min, anio_max):
    
    import pandas as pd
    # Filtrar el dataframe según el rango de años dado
    df_filtrado = df[(df[col_anio] >= anio_min) & (df[col_anio] <= anio_max)]
    
    # Establecer el multiíndice
    df_multi = df_filtrado.set_index(cols_cat + [col_anio])
    
    # Crear un índice completo para los años dentro del rango
    idx_completo = pd.MultiIndex.from_product([df[col].unique() for col in cols_cat] + [range(anio_min, anio_max + 1)], 
                                              names=cols_cat + [col_anio])
    
    # Reindexar el dataframe con el multiíndice completo
    df_completo = df_multi.reindex(idx_completo)
    
    # Rellenar valores nulos con el valor previo no nulo en la columna 'col_valor'
    df_completo[col_valor] = df_completo[col_valor].ffill()
    
    # Resetear el índice si se desea regresar a un dataframe plano
    df_completo = df_completo.reset_index()
    
    return df_completo
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
agregacion_suma(group_cols=['region_pbg', 'anio'], col_sum='participacion_vab'),
	rename_cols(map={'region_pbg': 'indicador', 'participacion_vab': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	completar_datos_faltantes(col_anio='anio', cols_cat=['indicador'], col_valor='valor', anio_min=1895, anio_max=2022)
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
#  |  0 | Cuyo        |   1895 |     7.2 |
#  
#  ------------------------------
#  
#  completar_datos_faltantes(col_anio='anio', cols_cat=['indicador'], col_valor='valor', anio_min=1895, anio_max=2022)
#  RangeIndex: 640 entries, 0 to 639
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  640 non-null    object 
#   1   anio       640 non-null    int64  
#   2   valor      640 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cuyo        |   1895 |     7.2 |
#  
#  ------------------------------
#  