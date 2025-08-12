from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'sector_desc': 'indicador', 'share_empleo': 'valor'}),
	drop_col(col=['gran_sector', 'sector_codigo', 'empleo_miles'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100),
	drop_na(col='valor')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  42228 non-null  object 
#   1   geonombreFundar  42228 non-null  object 
#   2   anio             42228 non-null  int64  
#   3   gran_sector      42228 non-null  object 
#   4   sector_codigo    42228 non-null  object 
#   5   sector_desc      42228 non-null  object 
#   6   empleo_miles     30958 non-null  float64
#   7   share_empleo     30953 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | gran_sector   | sector_codigo   | sector_desc   |   empleo_miles |   share_empleo |
#  |---:|:------------------|:------------------|-------:|:--------------|:----------------|:--------------|---------------:|---------------:|
#  |  0 | ARG               | Argentina         |   1950 | Bienes        | A               | Agro y pesca  |        1676.85 |       0.258262 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector_desc': 'indicador', 'share_empleo': 'valor'})
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  42228 non-null  object 
#   1   geonombreFundar  42228 non-null  object 
#   2   anio             42228 non-null  int64  
#   3   gran_sector      42228 non-null  object 
#   4   sector_codigo    42228 non-null  object 
#   5   indicador        42228 non-null  object 
#   6   empleo_miles     30958 non-null  float64
#   7   valor            30953 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | gran_sector   | sector_codigo   | indicador    |   empleo_miles |    valor |
#  |---:|:------------------|:------------------|-------:|:--------------|:----------------|:-------------|---------------:|---------:|
#  |  0 | ARG               | Argentina         |   1950 | Bienes        | A               | Agro y pesca |        1676.85 | 0.258262 |
#  
#  ------------------------------
#  
#  drop_col(col=['gran_sector', 'sector_codigo', 'empleo_miles'], axis=1)
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  42228 non-null  object 
#   1   geonombreFundar  42228 non-null  object 
#   2   anio             42228 non-null  int64  
#   3   indicador        42228 non-null  object 
#   4   valor            30953 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador    |   valor |
#  |---:|:------------------|:------------------|-------:|:-------------|--------:|
#  |  0 | ARG               | Argentina         |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  42228 non-null  object 
#   1   geonombreFundar  42228 non-null  object 
#   2   anio             42228 non-null  int64  
#   3   indicador        42228 non-null  object 
#   4   valor            30953 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador    |   valor |
#  |---:|:------------------|:------------------|-------:|:-------------|--------:|
#  |  0 | ARG               | Argentina         |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 30953 entries, 0 to 42227
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  30953 non-null  object 
#   1   geonombreFundar  30953 non-null  object 
#   2   anio             30953 non-null  int64  
#   3   indicador        30953 non-null  object 
#   4   valor            30953 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador    |   valor |
#  |---:|:------------------|:------------------|-------:|:-------------|--------:|
#  |  0 | ARG               | Argentina         |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  