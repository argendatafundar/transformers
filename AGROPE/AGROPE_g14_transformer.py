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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	rename_cols(map={'iso3': 'geocodigo'}),
	query(condition='geocodigoFundar != "F351" ')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  224 non-null    object 
#   1   geonombreFundar  224 non-null    object 
#   2   anio             224 non-null    int64  
#   3   valor            224 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             |   2021 |       2 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  224 non-null    object 
#   1   geonombreFundar  224 non-null    object 
#   2   anio             224 non-null    int64  
#   3   valor            224 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             |   2021 |       2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  Index: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  224 non-null    object 
#   1   geonombreFundar  224 non-null    object 
#   2   anio             224 non-null    int64  
#   3   valor            224 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             |   2021 |       2 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar != "F351" ')
#  Index: 223 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  223 non-null    object 
#   1   geonombreFundar  223 non-null    object 
#   2   anio             223 non-null    int64  
#   3   valor            223 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             |   2021 |       2 |
#  
#  ------------------------------
#  