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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'pais_nombre': 'categoria', 'participacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           48 non-null     object 
#   1   pais_nombre    64 non-null     object 
#   2   anio           64 non-null     int64  
#   3   participacion  64 non-null     float64
#  
#  |    |   iso3 | pais_nombre   |   anio |   participacion |
#  |---:|-------:|:--------------|-------:|----------------:|
#  |  0 |    nan | Resto LATAM   |   1820 |        0.405228 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   pais_nombre    64 non-null     object 
#   1   anio           64 non-null     int64  
#   2   participacion  64 non-null     float64
#  
#  |    | pais_nombre   |   anio |   participacion |
#  |---:|:--------------|-------:|----------------:|
#  |  0 | Resto LATAM   |   1820 |        0.405228 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pais_nombre': 'categoria', 'participacion': 'valor'})
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  64 non-null     object 
#   1   anio       64 non-null     int64  
#   2   valor      64 non-null     float64
#  
#  |    | categoria   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | Resto LATAM |   1820 | 0.405228 |
#  
#  ------------------------------
#  