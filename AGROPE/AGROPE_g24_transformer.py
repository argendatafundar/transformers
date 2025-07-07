from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'year': 'anio', 'share_expo': 'valor'}),
	query(condition="geocodigoFundar != 'F351'"),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  60 non-null     object 
#   1   geonombreFundar  60 non-null     object 
#   2   year             60 non-null     int64  
#   3   share_expo       60 non-null     float64
#   4   product          60 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year |   share_expo | product                                    |
#  |---:|:------------------|:------------------|-------:|-------------:|:-------------------------------------------|
#  |  0 | ARG               | Argentina         |   1962 |     0.110198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'share_expo': 'valor'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  60 non-null     object 
#   1   geonombreFundar  60 non-null     object 
#   2   anio             60 non-null     int64  
#   3   valor            60 non-null     float64
#   4   product          60 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |    valor | product                                    |
#  |---:|:------------------|:------------------|-------:|---------:|:-------------------------------------------|
#  |  0 | ARG               | Argentina         |   1962 | 0.110198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  60 non-null     object 
#   1   geonombreFundar  60 non-null     object 
#   2   anio             60 non-null     int64  
#   3   valor            60 non-null     float64
#   4   product          60 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | product                                    |
#  |---:|:------------------|:------------------|-------:|--------:|:-------------------------------------------|
#  |  0 | ARG               | Argentina         |   1962 | 11.0198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  60 non-null     object 
#   1   geonombreFundar  60 non-null     object 
#   2   anio             60 non-null     int64  
#   3   valor            60 non-null     float64
#   4   product          60 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor | product                                    |
#  |---:|:------------------|:------------------|-------:|--------:|:-------------------------------------------|
#  |  0 | ARG               | Argentina         |   1962 | 11.0198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  