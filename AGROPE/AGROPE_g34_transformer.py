from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
rename_cols(map={'cadena': 'categoria'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 651 entries, 0 to 650
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   cadena  651 non-null    object 
#   1   anio    651 non-null    int64  
#   2   valor   651 non-null    float64
#  
#  |    | cadena   |   anio |       valor |
#  |---:|:---------|-------:|------------:|
#  |  0 | Ajo      |   2001 | 2.45493e+08 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cadena': 'categoria'})
#  RangeIndex: 651 entries, 0 to 650
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  651 non-null    object 
#   1   anio       651 non-null    int64  
#   2   valor      651 non-null    float64
#  
#  |    | categoria   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | Ajo         |   2001 | 2.45493e+10 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 651 entries, 0 to 650
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  651 non-null    object 
#   1   anio       651 non-null    int64  
#   2   valor      651 non-null    float64
#  
#  |    | categoria   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | Ajo         |   2001 | 2.45493e+10 |
#  
#  ------------------------------
#  