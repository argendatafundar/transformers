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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador'}),
	rename_cols(map={'prop': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   prop    24 non-null     float64
#  
#  |    | sector                                                                                      |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   prop       24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop': 'valor'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   valor      24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |     valor |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  