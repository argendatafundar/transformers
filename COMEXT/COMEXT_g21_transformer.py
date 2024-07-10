from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def reemplazar_valor(df: DataFrame, col:str, query:str, nuevo_valor):
    indice = df.query(query).index
    df.loc[indice, col] = nuevo_valor
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
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
reemplazar_valor(col='iso3', query='descripcionpais == "Resto"', nuevo_valor='ROW'),
	rename_cols(map={'year': 'anio', 'iso3': 'geocodigo', 'export_value_pc': 'valor'}),
	drop_col(col='descripcionpais', axis=1),
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   year             168 non-null    int64  
#   1   iso3             168 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   export_value_pc  168 non-null    float64
#  
#  |    |   year | iso3   | descripcionpais   |   export_value_pc |
#  |---:|-------:|:-------|:------------------|------------------:|
#  |  0 |   2015 | DEU    | Alemania          |           1.84376 |
#  
#  ------------------------------
#  
#  reemplazar_valor(col='iso3', query='descripcionpais == "Resto"', nuevo_valor='ROW')
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   year             168 non-null    int64  
#   1   iso3             168 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   export_value_pc  168 non-null    float64
#  
#  |    |   year | iso3   | descripcionpais   |   export_value_pc |
#  |---:|-------:|:-------|:------------------|------------------:|
#  |  0 |   2015 | DEU    | Alemania          |           1.84376 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'iso3': 'geocodigo', 'export_value_pc': 'valor'})
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             168 non-null    int64  
#   1   geocodigo        168 non-null    object 
#   2   descripcionpais  168 non-null    object 
#   3   valor            168 non-null    float64
#  
#  |    |   anio | geocodigo   | descripcionpais   |   valor |
#  |---:|-------:|:------------|:------------------|--------:|
#  |  0 |   2015 | DEU         | Alemania          | 1.84376 |
#  
#  ------------------------------
#  
#  drop_col(col='descripcionpais', axis=1)
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       168 non-null    int64  
#   1   geocodigo  168 non-null    object 
#   2   valor      168 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2015 | DEU         | 1.84376 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 21 entries, 147 to 167
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   geocodigo  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |     |   anio | geocodigo   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 147 |   2022 | DEU         | 2.15493 |
#  
#  ------------------------------
#  