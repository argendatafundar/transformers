from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
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
	to_pandas(dummy=True),
	query(condition='anio == 2022'),
	rename_cols(map={'letra_desc_abrev': 'indicador', 'gran_region_desc': 'categoria', 'share_vab_sectorial': 'valor'}),
	drop_col(col=['anio', 'gran_region_id'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values_by_comparison(colname='letra', precedence={'B': 0, 'C': 1, 'D': 2, 'E': 3, 'F': 4, 'G': 5, 'H': 6, 'I': 7, 'J': 8, 'K': 9, 'L': 10, 'M': 11, 'N': 12, 'O': 13, 'P': 14, 'A': 15}, prefix=['categoria'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
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
#  drop_col(col=['anio', 'gran_region_id'], axis=1)
#  Index: 48 entries, 18 to 911
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   letra      48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   categoria  48 non-null     object 
#   3   valor      48 non-null     float64
#  
#  |    | letra   | indicador              | categoria   |   valor |
#  |---:|:--------|:-----------------------|:------------|--------:|
#  | 18 | L       | Adm. pública y defensa | Centro      | 5.04198 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 48 entries, 18 to 911
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   letra      48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   categoria  48 non-null     object 
#   3   valor      48 non-null     float64
#  
#  |    | letra   | indicador              | categoria   |   valor |
#  |---:|:--------|:-----------------------|:------------|--------:|
#  | 18 | L       | Adm. pública y defensa | Centro      | 5.04198 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='letra', precedence={'B': 0, 'C': 1, 'D': 2, 'E': 3, 'F': 4, 'G': 5, 'H': 6, 'I': 7, 'J': 8, 'K': 9, 'L': 10, 'M': 11, 'N': 12, 'O': 13, 'P': 14, 'A': 15}, prefix=['categoria'], suffix=[])
#  Index: 48 entries, 246 to 645
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   letra      48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   categoria  48 non-null     object 
#   3   valor      48 non-null     float64
#  
#  |     | letra   | indicador   | categoria   |    valor |
#  |----:|:--------|:------------|:------------|---------:|
#  | 246 | B       | Pesca       | Centro      | 0.182441 |
#  
#  ------------------------------
#  