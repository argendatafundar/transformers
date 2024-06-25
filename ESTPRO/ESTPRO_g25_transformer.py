from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio != 2020'),
	query(condition='intensidad_id_ocde == "Media y alta intensidad de I+D" & anio == anio.max()'),
	rename_cols(map={'share': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['anio', 'intensidad_id_ocde', 'empleo'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3640 entries, 0 to 3639
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                3640 non-null   object 
#   1   anio                3640 non-null   int64  
#   2   intensidad_id_ocde  3640 non-null   object 
#   3   empleo              3640 non-null   float64
#   4   share               3640 non-null   float64
#  
#  |    | iso3   |   anio | intensidad_id_ocde     |   empleo |    share |
#  |---:|:-------|-------:|:-----------------------|---------:|---------:|
#  |  0 | ARG    |   1995 | Baja intensidad de I+D |  11239.8 | 0.915144 |
#  
#  ------------------------------
#  
#  query(condition='anio != 2020')
#  Index: 3500 entries, 0 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                3500 non-null   object 
#   1   anio                3500 non-null   int64  
#   2   intensidad_id_ocde  3500 non-null   object 
#   3   empleo              3500 non-null   float64
#   4   share               3500 non-null   float64
#  
#  |    | iso3   |   anio | intensidad_id_ocde     |   empleo |    share |
#  |---:|:-------|-------:|:-----------------------|---------:|---------:|
#  |  0 | ARG    |   1995 | Baja intensidad de I+D |  11239.8 | 0.915144 |
#  
#  ------------------------------
#  
#  query(condition='intensidad_id_ocde == "Media y alta intensidad de I+D" & anio == anio.max()')
#  Index: 70 entries, 49 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                70 non-null     object 
#   1   anio                70 non-null     int64  
#   2   intensidad_id_ocde  70 non-null     object 
#   3   empleo              70 non-null     float64
#   4   share               70 non-null     float64
#  
#  |    | iso3   |   anio | intensidad_id_ocde             |   empleo |     share |
#  |---:|:-------|-------:|:-------------------------------|---------:|----------:|
#  | 49 | ARG    |   2019 | Media y alta intensidad de I+D |   1771.7 | 0.0849252 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share': 'valor', 'iso3': 'geocodigo'})
#  Index: 70 entries, 49 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigo           70 non-null     object 
#   1   anio                70 non-null     int64  
#   2   intensidad_id_ocde  70 non-null     object 
#   3   empleo              70 non-null     float64
#   4   valor               70 non-null     float64
#  
#  |    | geocodigo   |   anio | intensidad_id_ocde             |   empleo |     valor |
#  |---:|:------------|-------:|:-------------------------------|---------:|----------:|
#  | 49 | ARG         |   2019 | Media y alta intensidad de I+D |   1771.7 | 0.0849252 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'intensidad_id_ocde', 'empleo'], axis=1)
#  Index: 70 entries, 49 to 3637
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  70 non-null     object 
#   1   valor      70 non-null     float64
#  
#  |    | geocodigo   |     valor |
#  |---:|:------------|----------:|
#  | 49 | ARG         | 0.0849252 |
#  
#  ------------------------------
#  