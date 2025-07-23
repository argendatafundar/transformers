from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'prop_expo': 'valor'}),
	sort_values(how='ascending', by=['anio', 'geonombreFundar']),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3194 non-null   object 
#   1   geonombreFundar  3194 non-null   object 
#   2   anio             3194 non-null   int64  
#   3   prop_expo        3194 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prop_expo |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AFG               | Afganistán        |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_expo': 'valor'})
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3194 non-null   object 
#   1   geonombreFundar  3194 non-null   object 
#   2   anio             3194 non-null   int64  
#   3   valor            3194 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AFG               | Afganistán        |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geonombreFundar'])
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3194 non-null   object 
#   1   geonombreFundar  3194 non-null   object 
#   2   anio             3194 non-null   int64  
#   3   valor            3194 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |     valor |
#  |---:|:------------------|:------------------|-------:|----------:|
#  |  0 | ALB               | Albania           |   2005 | 0.0070921 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3194 non-null   object 
#   1   geonombreFundar  3194 non-null   object 
#   2   anio             3194 non-null   int64  
#   3   valor            3194 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |     valor |
#  |---:|:------------------|:------------------|-------:|----------:|
#  |  0 | ALB               | Albania           |   2005 | 0.0070921 |
#  
#  ------------------------------
#  