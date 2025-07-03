from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'rindes_maiz_ma5': 'valor'}),
	query(condition='anio >= 1965'),
	drop_na(col=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	query(condition="geocodigoFundar != 'F351'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9403 non-null   object 
#   1   geonombreFundar  9403 non-null   object 
#   2   anio             9403 non-null   int64  
#   3   rindes           9403 non-null   float64
#   4   rindes_maiz_ma5  9403 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:------------------|:------------------|-------:|---------:|:------------------|
#  |  0 | AFG               | Afganistán        |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  rename_cols(map={'rindes_maiz_ma5': 'valor'})
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9403 non-null   object 
#   1   geonombreFundar  9403 non-null   object 
#   2   anio             9403 non-null   int64  
#   3   rindes           9403 non-null   float64
#   4   valor            9403 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | valor   |
#  |---:|:------------------|:------------------|-------:|---------:|:--------|
#  |  0 | AFG               | Afganistán        |   1961 |      1.4 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 8847 entries, 4 to 9402
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  8847 non-null   object 
#   1   geonombreFundar  8847 non-null   object 
#   2   anio             8847 non-null   int64  
#   3   rindes           8847 non-null   float64
#   4   valor            8847 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  4 | AFG               | Afganistán        |   1965 |     1.44 | 1.41834 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 8847 entries, 4 to 9402
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  8847 non-null   object 
#   1   geonombreFundar  8847 non-null   object 
#   2   anio             8847 non-null   int64  
#   3   rindes           8847 non-null   float64
#   4   valor            8847 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  4 | AFG               | Afganistán        |   1965 |     1.44 | 1.41834 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  RangeIndex: 8847 entries, 0 to 8846
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  8847 non-null   object 
#   1   geonombreFundar  8847 non-null   object 
#   2   anio             8847 non-null   int64  
#   3   rindes           8847 non-null   float64
#   4   valor            8847 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | AFG               | Afganistán        |   1965 |     1.44 | 1.41834 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 8815 entries, 0 to 8846
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  8815 non-null   object 
#   1   geonombreFundar  8815 non-null   object 
#   2   anio             8815 non-null   int64  
#   3   rindes           8815 non-null   float64
#   4   valor            8815 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | AFG               | Afganistán        |   1965 |     1.44 | 1.41834 |
#  
#  ------------------------------
#  