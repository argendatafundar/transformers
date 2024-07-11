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
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               76 non-null     int64  
#   1   cuenta             76 non-null     object 
#   2   participacion_pbi  76 non-null     float64
#  
#  |    |   anio | cuenta      |   participacion_pbi |
#  |---:|-------:|:------------|--------------------:|
#  |  0 |   2004 | Agricultura |                0.05 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cuenta': 'indicador', 'participacion_pbi': 'valor'})
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'Total'")
#  Index: 57 entries, 0 to 74
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       57 non-null     int64  
#   1   indicador  57 non-null     object 
#   2   valor      57 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  