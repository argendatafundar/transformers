from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'cuenta': 'indicador', 'participacion_pbi': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio']),
	query(condition="indicador != 'Total'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               80 non-null     int64  
#   1   cuenta             80 non-null     object 
#   2   participacion_pbi  80 non-null     float64
#  
#  |    |   anio | cuenta      |   participacion_pbi |
#  |---:|-------:|:------------|--------------------:|
#  |  0 |   2004 | Agricultura |           0.0496514 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cuenta': 'indicador', 'participacion_pbi': 'valor'})
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       80 non-null     int64  
#   1   indicador  80 non-null     object 
#   2   valor      80 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura | 4.96514 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       80 non-null     int64  
#   1   indicador  80 non-null     object 
#   2   valor      80 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura | 4.96514 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       80 non-null     int64  
#   1   indicador  80 non-null     object 
#   2   valor      80 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura | 4.96514 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 60 entries, 0 to 78
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       60 non-null     int64  
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura | 4.96514 |
#  
#  ------------------------------
#  