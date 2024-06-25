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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2022'),
	rename_cols(map={'letra_desc_abrev': 'indicador', 'gran_region_desc': 'categoria', 'share_vab_sectorial': 'valor'}),
	drop_col(col=['letra', 'anio', 'gran_region_id'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 912 entries, 0 to 911
#  Data columns (total 6 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 912 non-null    int64  
#   1   letra                912 non-null    object 
#   2   letra_desc_abrev     912 non-null    object 
#   3   gran_region_id       912 non-null    int64  
#   4   gran_region_desc     912 non-null    object 
#   5   share_vab_sectorial  912 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev       |   gran_region_id | gran_region_desc   |   share_vab_sectorial |
#  |---:|-------:|:--------|:-----------------------|-----------------:|:-------------------|----------------------:|
#  |  0 |   2004 | L       | Adm. pública y defensa |                2 | Centro             |              0.047162 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2022')
#  Index: 48 entries, 18 to 911
#  Data columns (total 6 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 48 non-null     int64  
#   1   letra                48 non-null     object 
#   2   letra_desc_abrev     48 non-null     object 
#   3   gran_region_id       48 non-null     int64  
#   4   gran_region_desc     48 non-null     object 
#   5   share_vab_sectorial  48 non-null     float64
#  
#  |    |   anio | letra   | letra_desc_abrev       |   gran_region_id | gran_region_desc   |   share_vab_sectorial |
#  |---:|-------:|:--------|:-----------------------|-----------------:|:-------------------|----------------------:|
#  | 18 |   2022 | L       | Adm. pública y defensa |                2 | Centro             |             0.0504198 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'indicador', 'gran_region_desc': 'categoria', 'share_vab_sectorial': 'valor'})
#  Index: 48 entries, 18 to 911
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            48 non-null     int64  
#   1   letra           48 non-null     object 
#   2   indicador       48 non-null     object 
#   3   gran_region_id  48 non-null     int64  
#   4   categoria       48 non-null     object 
#   5   valor           48 non-null     float64
#  
#  |    |   anio | letra   | indicador              |   gran_region_id | categoria   |     valor |
#  |---:|-------:|:--------|:-----------------------|-----------------:|:------------|----------:|
#  | 18 |   2022 | L       | Adm. pública y defensa |                2 | Centro      | 0.0504198 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'anio', 'gran_region_id'], axis=1)
#  Index: 48 entries, 18 to 911
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  48 non-null     object 
#   1   categoria  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | indicador              | categoria   |   valor |
#  |---:|:-----------------------|:------------|--------:|
#  | 18 | Adm. pública y defensa | Centro      | 5.04198 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 48 entries, 18 to 911
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  48 non-null     object 
#   1   categoria  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | indicador              | categoria   |   valor |
#  |---:|:-----------------------|:------------|--------:|
#  | 18 | Adm. pública y defensa | Centro      | 5.04198 |
#  
#  ------------------------------
#  