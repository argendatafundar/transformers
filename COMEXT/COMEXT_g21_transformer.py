from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def reemplazar_valor(df: DataFrame, col:str, query:str, nuevo_valor):
    indice = df.query(query).index
    df.loc[indice, col] = nuevo_valor
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	reemplazar_valor(col='descripcionpais', query='descripcionpais == "Resto"', nuevo_valor='Resto del mundo'),
	rename_cols(map={'year': 'anio', 'geocodigoFundar': 'geocodigo', 'export_value_pc': 'valor'}),
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   year             168 non-null    int64  
#   1   iso3             160 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   export_value_pc  168 non-null    float64
#   4   geocodigoFundar  160 non-null    object 
#   5   geonombreFundar  160 non-null    object 
#  
#  |    |   year | iso3   | descripcionpais   |   export_value_pc | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:-------|:------------------|------------------:|:------------------|:------------------|
#  |  0 |   2015 | DEU    | Alemania          |           1.84376 | DEU               | Alemania          |
#  
#  ------------------------------
#  
#  reemplazar_valor(col='descripcionpais', query='descripcionpais == "Resto"', nuevo_valor='Resto del mundo')
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   year             168 non-null    int64  
#   1   iso3             160 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   export_value_pc  168 non-null    float64
#   4   geocodigoFundar  160 non-null    object 
#   5   geonombreFundar  160 non-null    object 
#  
#  |    |   year | iso3   | descripcionpais   |   export_value_pc | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:-------|:------------------|------------------:|:------------------|:------------------|
#  |  0 |   2015 | DEU    | Alemania          |           1.84376 | DEU               | Alemania          |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'geocodigoFundar': 'geocodigo', 'export_value_pc': 'valor'})
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             168 non-null    int64  
#   1   iso3             160 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   valor            168 non-null    float64
#   4   geocodigo        160 non-null    object 
#   5   geonombreFundar  160 non-null    object 
#  
#  |    |   anio | iso3   | descripcionpais   |   valor | geocodigo   | geonombreFundar   |
#  |---:|-------:|:-------|:------------------|--------:|:------------|:------------------|
#  |  0 |   2015 | DEU    | Alemania          | 1.84376 | DEU         | Alemania          |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 21 entries, 147 to 167
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             21 non-null     int64  
#   1   iso3             20 non-null     object 
#   2   descripcionpais  21 non-null     object 
#   3   valor            21 non-null     float64
#   4   geocodigo        20 non-null     object 
#   5   geonombreFundar  20 non-null     object 
#  
#  |     |   anio | iso3   | descripcionpais   |   valor | geocodigo   | geonombreFundar   |
#  |----:|-------:|:-------|:------------------|--------:|:------------|:------------------|
#  | 147 |   2022 | DEU    | Alemania          | 2.15493 | DEU         | Alemania          |
#  
#  ------------------------------
#  