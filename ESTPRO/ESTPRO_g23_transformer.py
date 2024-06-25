from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tamanio_desc': 'categoria', 'tasa_informalidad': 'valor'}),
	drop_col(col=['tamanio_cod'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               126 non-null    int64  
#   1   tamanio_cod        126 non-null    int64  
#   2   tamanio_desc       126 non-null    object 
#   3   tasa_informalidad  126 non-null    float64
#  
#  |    |   anio |   tamanio_cod | tamanio_desc     |   tasa_informalidad |
#  |---:|-------:|--------------:|:-----------------|--------------------:|
#  |  0 |   2003 |             1 | Hasta 5 ocupados |            0.759928 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tamanio_desc': 'categoria', 'tasa_informalidad': 'valor'})
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         126 non-null    int64  
#   1   tamanio_cod  126 non-null    int64  
#   2   categoria    126 non-null    object 
#   3   valor        126 non-null    float64
#  
#  |    |   anio |   tamanio_cod | categoria        |    valor |
#  |---:|-------:|--------------:|:-----------------|---------:|
#  |  0 |   2003 |             1 | Hasta 5 ocupados | 0.759928 |
#  
#  ------------------------------
#  
#  drop_col(col=['tamanio_cod'], axis=1)
#  RangeIndex: 126 entries, 0 to 125
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       126 non-null    int64  
#   1   categoria  126 non-null    object 
#   2   valor      126 non-null    float64
#  
#  |    |   anio | categoria        |    valor |
#  |---:|-------:|:-----------------|---------:|
#  |  0 |   2003 | Hasta 5 ocupados | 0.759928 |
#  
#  ------------------------------
#  