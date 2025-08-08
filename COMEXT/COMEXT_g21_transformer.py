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
	reemplazar_valor(col='geocodigoFundar', query='geonombreFundar == "Resto"', nuevo_valor='ROW'),
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
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  160 non-null    object 
#   1   geonombreFundar  160 non-null    object 
#   2   year             168 non-null    int64  
#   3   export_value_pc  168 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|------------------:|
#  |  0 | DEU               | Alemania          |   2015 |           1.84376 |
#  
#  ------------------------------
#  
#  reemplazar_valor(col='geocodigoFundar', query='geonombreFundar == "Resto"', nuevo_valor='ROW')
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  160 non-null    object 
#   1   geonombreFundar  160 non-null    object 
#   2   year             168 non-null    int64  
#   3   export_value_pc  168 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|------------------:|
#  |  0 | DEU               | Alemania          |   2015 |           1.84376 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'geocodigoFundar': 'geocodigo', 'export_value_pc': 'valor'})
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        160 non-null    object 
#   1   geonombreFundar  160 non-null    object 
#   2   anio             168 non-null    int64  
#   3   valor            168 non-null    float64
#  
#  |    | geocodigo   | geonombreFundar   |   anio |   valor |
#  |---:|:------------|:------------------|-------:|--------:|
#  |  0 | DEU         | Alemania          |   2015 | 1.84376 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 21 entries, 147 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   anio             21 non-null     int64  
#   3   valor            21 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio |   valor |
#  |----:|:------------|:------------------|-------:|--------:|
#  | 147 | DEU         | Alemania          |   2022 | 2.15493 |
#  
#  ------------------------------
#  