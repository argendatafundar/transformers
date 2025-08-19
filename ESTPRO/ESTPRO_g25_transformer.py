from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio != 2020'),
	query(condition='anio != 2021'),
	latest_year(by='anio'),
	query(condition='intensidad_id_ocde == "Media y alta intensidad de I+D"'),
	rename_cols(map={'share': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['intensidad_id_ocde', 'empleo'], axis=1),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 3640 entries, 0 to 3639
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     3640 non-null   object 
#   1   geonombreFundar     3640 non-null   object 
#   2   anio                3640 non-null   int64  
#   3   intensidad_id_ocde  3640 non-null   object 
#   4   empleo              3640 non-null   float64
#   5   share               3640 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | intensidad_id_ocde     |   empleo |    share |
#  |---:|:------------------|:------------------|-------:|:-----------------------|---------:|---------:|
#  |  0 | ARG               | Argentina         |   1995 | Baja intensidad de I+D |  11239.8 | 0.915144 |
#  
#  ------------------------------
#  
#  query(condition='anio != 2020')
#  Index: 3500 entries, 0 to 3637
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     3500 non-null   object 
#   1   geonombreFundar     3500 non-null   object 
#   2   anio                3500 non-null   int64  
#   3   intensidad_id_ocde  3500 non-null   object 
#   4   empleo              3500 non-null   float64
#   5   share               3500 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | intensidad_id_ocde     |   empleo |    share |
#  |---:|:------------------|:------------------|-------:|:-----------------------|---------:|---------:|
#  |  0 | ARG               | Argentina         |   1995 | Baja intensidad de I+D |  11239.8 | 0.915144 |
#  
#  ------------------------------
#  
#  query(condition='anio != 2021')
#  Index: 3500 entries, 0 to 3637
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     3500 non-null   object 
#   1   geonombreFundar     3500 non-null   object 
#   2   anio                3500 non-null   int64  
#   3   intensidad_id_ocde  3500 non-null   object 
#   4   empleo              3500 non-null   float64
#   5   share               3500 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | intensidad_id_ocde     |   empleo |    share |
#  |---:|:------------------|:------------------|-------:|:-----------------------|---------:|---------:|
#  |  0 | ARG               | Argentina         |   1995 | Baja intensidad de I+D |  11239.8 | 0.915144 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 140 entries, 48 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     140 non-null    object 
#   1   geonombreFundar     140 non-null    object 
#   2   intensidad_id_ocde  140 non-null    object 
#   3   empleo              140 non-null    float64
#   4   share               140 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde     |   empleo |    share |
#  |---:|:------------------|:------------------|:-----------------------|---------:|---------:|
#  | 48 | ARG               | Argentina         | Baja intensidad de I+D |  19090.2 | 0.915075 |
#  
#  ------------------------------
#  
#  query(condition='intensidad_id_ocde == "Media y alta intensidad de I+D"')
#  Index: 70 entries, 49 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     70 non-null     object 
#   1   geonombreFundar     70 non-null     object 
#   2   intensidad_id_ocde  70 non-null     object 
#   3   empleo              70 non-null     float64
#   4   share               70 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde             |   empleo |     share |
#  |---:|:------------------|:------------------|:-------------------------------|---------:|----------:|
#  | 49 | ARG               | Argentina         | Media y alta intensidad de I+D |   1771.7 | 0.0849252 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share': 'valor', 'iso3': 'geocodigo'})
#  Index: 70 entries, 49 to 3637
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     70 non-null     object 
#   1   geonombreFundar     70 non-null     object 
#   2   intensidad_id_ocde  70 non-null     object 
#   3   empleo              70 non-null     float64
#   4   valor               70 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde             |   empleo |     valor |
#  |---:|:------------------|:------------------|:-------------------------------|---------:|----------:|
#  | 49 | ARG               | Argentina         | Media y alta intensidad de I+D |   1771.7 | 0.0849252 |
#  
#  ------------------------------
#  
#  drop_col(col=['intensidad_id_ocde', 'empleo'], axis=1)
#  Index: 70 entries, 49 to 3637
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  70 non-null     object 
#   1   geonombreFundar  70 non-null     object 
#   2   valor            70 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  | 49 | ARG               | Argentina         | 8.49252 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 70 entries, 49 to 3637
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  70 non-null     object 
#   1   geonombreFundar  70 non-null     object 
#   2   valor            70 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  | 49 | ARG               | Argentina         | 8.49252 |
#  
#  ------------------------------
#  