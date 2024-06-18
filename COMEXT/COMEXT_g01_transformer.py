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
rename_cols(map={'cantidades_exportacion_ferreres': 'valor'}),
	drop_col(col='cantidades_exportacion_indec', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             214 non-null    int64  
#   1   cantidades_exportacion_ferreres  209 non-null    float64
#   2   cantidades_exportacion_indec     38 non-null     float64
#  
#  |    |   anio |   cantidades_exportacion_ferreres |   cantidades_exportacion_indec |
#  |---:|-------:|----------------------------------:|-------------------------------:|
#  |  0 |   1810 |                         0.0193818 |                            nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'cantidades_exportacion_ferreres': 'valor'})
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 3 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          214 non-null    int64  
#   1   valor                         209 non-null    float64
#   2   cantidades_exportacion_indec  38 non-null     float64
#  
#  |    |   anio |     valor |   cantidades_exportacion_indec |
#  |---:|-------:|----------:|-------------------------------:|
#  |  0 |   1810 | 0.0193818 |                            nan |
#  
#  ------------------------------
#  
#  drop_col(col='cantidades_exportacion_indec', axis=1)
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    214 non-null    int64  
#   1   valor   209 non-null    float64
#  
#  |    |   anio |     valor |
#  |---:|-------:|----------:|
#  |  0 |   1810 | 0.0193818 |
#  
#  ------------------------------
#  