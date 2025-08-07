from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def case_when_na(df: DataFrame, col_na:str, col_replace:str, col_name:str = None ): 
    nan_rows = df[col_na].isna()

    new_col = col_name
    if col_name is None: 
        new_col = col_na

    df[new_col] = df[col_na]    
    df.loc[nan_rows, new_col] = df.loc[nan_rows, col_replace]
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	case_when_na(col_na='cantidades_exportacion_ferreres', col_replace='cantidades_exportacion_indec', col_name='valor'),
	drop_col(col=['cantidades_exportacion_ferreres', 'cantidades_exportacion_indec'], axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             214 non-null    int64  
#   1   cantidades_exportacion_ferreres  209 non-null    float64
#   2   cantidades_exportacion_indec     38 non-null     object 
#   3   valor                            214 non-null    object 
#  
#  |    |   anio |   cantidades_exportacion_ferreres | cantidades_exportacion_indec   |     valor |
#  |---:|-------:|----------------------------------:|:-------------------------------|----------:|
#  |  0 |   1810 |                         0.0193818 |                                | 0.0193818 |
#  
#  ------------------------------
#  
#  case_when_na(col_na='cantidades_exportacion_ferreres', col_replace='cantidades_exportacion_indec', col_name='valor')
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             214 non-null    int64  
#   1   cantidades_exportacion_ferreres  209 non-null    float64
#   2   cantidades_exportacion_indec     38 non-null     object 
#   3   valor                            214 non-null    object 
#  
#  |    |   anio |   cantidades_exportacion_ferreres | cantidades_exportacion_indec   |     valor |
#  |---:|-------:|----------------------------------:|:-------------------------------|----------:|
#  |  0 |   1810 |                         0.0193818 |                                | 0.0193818 |
#  
#  ------------------------------
#  
#  drop_col(col=['cantidades_exportacion_ferreres', 'cantidades_exportacion_indec'], axis=1)
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype 
#  ---  ------  --------------  ----- 
#   0   anio    214 non-null    int64 
#   1   valor   214 non-null    object
#  
#  |    |   anio |     valor |
#  |---:|-------:|----------:|
#  |  0 |   1810 | 0.0193818 |
#  
#  ------------------------------
#  