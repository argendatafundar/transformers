from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def datetime_to_year(df, col: str):
    df[col] = pd.to_datetime(df[col]).dt.year
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'fecha': 'anio', 'anomalia_temperatura_deg_c': 'valor'}),
	datetime_to_year(col='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 171 entries, 0 to 170
#  Data columns (total 2 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   fecha                       171 non-null    object 
#   1   anomalia_temperatura_deg_c  171 non-null    float64
#  
#  |    | fecha      |   anomalia_temperatura_deg_c |
#  |---:|:-----------|-----------------------------:|
#  |  0 | 1850-01-01 |                       -0.059 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio', 'anomalia_temperatura_deg_c': 'valor'})
#  RangeIndex: 171 entries, 0 to 170
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    171 non-null    int32  
#   1   valor   171 non-null    float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1850 |  -0.059 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  RangeIndex: 171 entries, 0 to 170
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    171 non-null    int32  
#   1   valor   171 non-null    float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1850 |  -0.059 |
#  
#  ------------------------------
#  