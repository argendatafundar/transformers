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
drop_col(col='pais', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'brecha': 'valor', 'circa': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   iso3        22 non-null     object 
#   1   pais        22 non-null     object 
#   2   anio_circa  22 non-null     int64  
#   3   brecha      22 non-null     float64
#   4   circa       22 non-null     int64  
#  
#  |    | iso3   | pais      |   anio_circa |   brecha |   circa |
#  |---:|:-------|:----------|-------------:|---------:|--------:|
#  |  0 | ARG    | Argentina |         2000 |  13.9852 |    2000 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   iso3        22 non-null     object 
#   1   anio_circa  22 non-null     int64  
#   2   brecha      22 non-null     float64
#   3   circa       22 non-null     int64  
#  
#  |    | iso3   |   anio_circa |   brecha |   circa |
#  |---:|:-------|-------------:|---------:|--------:|
#  |  0 | ARG    |         2000 |  13.9852 |    2000 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'brecha': 'valor', 'circa': 'indicador'})
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   geocodigo   22 non-null     object 
#   1   anio_circa  22 non-null     int64  
#   2   valor       22 non-null     float64
#   3   indicador   22 non-null     int64  
#  
#  |    | geocodigo   |   anio_circa |   valor |   indicador |
#  |---:|:------------|-------------:|--------:|------------:|
#  |  0 | ARG         |         2000 | 13.9852 |        2000 |
#  
#  ------------------------------
#  