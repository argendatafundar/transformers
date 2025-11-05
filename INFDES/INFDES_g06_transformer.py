from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def imput_na(df:DataFrame, bool_mask:list[bool], col:str, value:Any):
    df.loc[bool_mask,col] = value
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=100),
	imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True], col='geonombreFundar', value='Nacional'),
	query(condition='anio == anio.max()'),
	drop_col(col=['geocodigoFundar', 'prov_cod'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 394 entries, 0 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    378 non-null    object 
#   1   geonombreFundar    378 non-null    object 
#   2   anio               394 non-null    int64  
#   3   prov_cod           394 non-null    int64  
#   4   tipo_informalidad  394 non-null    object 
#   5   valor              394 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |    valor |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|---------:|
#  |  0 | AR-C              | CABA              |   2016 |          2 | Informalidad (definición productiva) | 0.262605 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 394 entries, 0 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    378 non-null    object 
#   1   geonombreFundar    394 non-null    object 
#   2   anio               394 non-null    int64  
#   3   prov_cod           394 non-null    int64  
#   4   tipo_informalidad  394 non-null    object 
#   5   valor              394 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |   valor |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|--------:|
#  |  0 | AR-C              | CABA              |   2016 |          2 | Informalidad (definición productiva) | 26.2605 |
#  
#  ------------------------------
#  
#  imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True], col='geonombreFundar', value='Nacional')
#  RangeIndex: 394 entries, 0 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    378 non-null    object 
#   1   geonombreFundar    394 non-null    object 
#   2   anio               394 non-null    int64  
#   3   prov_cod           394 non-null    int64  
#   4   tipo_informalidad  394 non-null    object 
#   5   valor              394 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |   valor |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|--------:|
#  |  0 | AR-C              | CABA              |   2016 |          2 | Informalidad (definición productiva) | 26.2605 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 50 entries, 330 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    48 non-null     object 
#   1   geonombreFundar    50 non-null     object 
#   2   anio               50 non-null     int64  
#   3   prov_cod           50 non-null     int64  
#   4   tipo_informalidad  50 non-null     object 
#   5   valor              50 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |   valor |
#  |----:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|--------:|
#  | 330 | AR-C              | CABA              |   2023 |          2 | Informalidad (definición productiva) | 26.7745 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'prov_cod'], axis=1)
#  Index: 50 entries, 330 to 393
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    50 non-null     object 
#   1   anio               50 non-null     int64  
#   2   tipo_informalidad  50 non-null     object 
#   3   valor              50 non-null     float64
#  
#  |     | geonombreFundar   |   anio | tipo_informalidad                    |   valor |
#  |----:|:------------------|-------:|:-------------------------------------|--------:|
#  | 330 | CABA              |   2023 | Informalidad (definición productiva) | 26.7745 |
#  
#  ------------------------------
#  