from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'exportaciones': 'Exportaciones', 'importaciones': 'Importaciones'}),
	query(condition="sector == 'SBC'"),
	drop_col(col=['balanza', 'sector'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='categoria', values_to_col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           119 non-null    int64  
#   1   sector         119 non-null    object 
#   2   balanza        119 non-null    float64
#   3   exportaciones  119 non-null    float64
#   4   importaciones  119 non-null    float64
#  
#  |    |   anio | sector   |   balanza |   exportaciones |   importaciones |
#  |---:|-------:|:---------|----------:|----------------:|----------------:|
#  |  0 |   2006 | SBC      |   200.396 |         2521.42 |         2321.02 |
#  
#  ------------------------------
#  
#  rename_cols(map={'exportaciones': 'Exportaciones', 'importaciones': 'Importaciones'})
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           119 non-null    int64  
#   1   sector         119 non-null    object 
#   2   balanza        119 non-null    float64
#   3   Exportaciones  119 non-null    float64
#   4   Importaciones  119 non-null    float64
#  
#  |    |   anio | sector   |   balanza |   Exportaciones |   Importaciones |
#  |---:|-------:|:---------|----------:|----------------:|----------------:|
#  |  0 |   2006 | SBC      |   200.396 |         2521.42 |         2321.02 |
#  
#  ------------------------------
#  
#  query(condition="sector == 'SBC'")
#  Index: 17 entries, 0 to 16
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           17 non-null     int64  
#   1   sector         17 non-null     object 
#   2   balanza        17 non-null     float64
#   3   Exportaciones  17 non-null     float64
#   4   Importaciones  17 non-null     float64
#  
#  |    |   anio | sector   |   balanza |   Exportaciones |   Importaciones |
#  |---:|-------:|:---------|----------:|----------------:|----------------:|
#  |  0 |   2006 | SBC      |   200.396 |         2521.42 |         2321.02 |
#  
#  ------------------------------
#  
#  drop_col(col=['balanza', 'sector'], axis=1)
#  Index: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           17 non-null     int64  
#   1   Exportaciones  17 non-null     float64
#   2   Importaciones  17 non-null     float64
#  
#  |    |   anio |   Exportaciones |   Importaciones |
#  |---:|-------:|----------------:|----------------:|
#  |  0 |   2006 |         2521.42 |         2321.02 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='categoria', values_to_col='valor')
#  RangeIndex: 34 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       34 non-null     int64  
#   1   categoria  34 non-null     object 
#   2   valor      34 non-null     float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   2006 | Exportaciones | 2521.42 |
#  
#  ------------------------------
#  