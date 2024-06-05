from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'periodo': 'anio'}),
	rename_cols(map={'sector': 'indicador'}),
	rename_cols(map={'brecha': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   periodo  54 non-null     int64  
#   1   brecha   54 non-null     float64
#   2   sector   54 non-null     object 
#  
#  |    |   periodo |   brecha | sector            |
#  |---:|----------:|---------:|:------------------|
#  |  0 |      1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  
#  rename_cols(map={'periodo': 'anio'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    54 non-null     int64  
#   1   brecha  54 non-null     float64
#   2   sector  54 non-null     object 
#  
#  |    |   anio |   brecha | sector            |
#  |---:|-------:|---------:|:------------------|
#  |  0 |   1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   brecha     54 non-null     float64
#   2   indicador  54 non-null     object 
#  
#  |    |   anio |   brecha | indicador         |
#  |---:|-------:|---------:|:------------------|
#  |  0 |   1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  
#  rename_cols(map={'brecha': 'valor'})
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   indicador  54 non-null     object 
#  
#  |    |   anio |    valor | indicador         |
#  |---:|-------:|---------:|:------------------|
#  |  0 |   1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  