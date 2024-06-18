from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(sector='categoria', brecha='valor', periodo='anio')
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
#  rename_columns(sector='categoria', brecha='valor', periodo='anio')
#  RangeIndex: 54 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       54 non-null     int64  
#   1   valor      54 non-null     float64
#   2   categoria  54 non-null     object 
#  
#  |    |   anio |    valor | categoria         |
#  |---:|-------:|---------:|:------------------|
#  |  0 |   1996 | 0.237825 | Promedio economía |
#  
#  ------------------------------
#  