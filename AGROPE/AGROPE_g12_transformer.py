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
	rename_cols(map={'rindes_soja_ma5': 'valor'}),
	query(condition='anio >= 1965'),
	drop_na(col=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	query(condition="geocodigoFundar != 'F351'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4771 non-null   object 
#   1   geonombreFundar  4771 non-null   object 
#   2   anio             4771 non-null   int64  
#   3   rindes           4771 non-null   float64
#   4   rindes_soja_ma5  4771 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:------------------|:------------------|-------:|---------:|:------------------|
#  |  0 | AGO               | Angola            |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  rename_cols(map={'rindes_soja_ma5': 'valor'})
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4771 non-null   object 
#   1   geonombreFundar  4771 non-null   object 
#   2   anio             4771 non-null   int64  
#   3   rindes           4771 non-null   float64
#   4   valor            4771 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | valor   |
#  |---:|:------------------|:------------------|-------:|---------:|:--------|
#  |  0 | AGO               | Angola            |   2000 |   0.2333 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 4576 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4576 non-null   object 
#   1   geonombreFundar  4576 non-null   object 
#   2   anio             4576 non-null   int64  
#   3   rindes           4576 non-null   float64
#   4   valor            4576 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | valor   |
#  |---:|:------------------|:------------------|-------:|---------:|:--------|
#  |  0 | AGO               | Angola            |   2000 |   0.2333 | NA      |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 4576 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4576 non-null   object 
#   1   geonombreFundar  4576 non-null   object 
#   2   anio             4576 non-null   int64  
#   3   rindes           4576 non-null   float64
#   4   valor            4576 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | valor   |
#  |---:|:------------------|:------------------|-------:|---------:|:--------|
#  |  0 | AGO               | Angola            |   2000 |   0.2333 | NA      |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  RangeIndex: 4576 entries, 0 to 4575
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4576 non-null   object 
#   1   geonombreFundar  4576 non-null   object 
#   2   anio             4576 non-null   int64  
#   3   rindes           4576 non-null   float64
#   4   valor            4576 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | ARG               | Argentina         |   1965 |   1.0352 | 1.06008 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 4544 entries, 0 to 4575
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4544 non-null   object 
#   1   geonombreFundar  4544 non-null   object 
#   2   anio             4544 non-null   int64  
#   3   rindes           4544 non-null   float64
#   4   valor            4544 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | ARG               | Argentina         |   1965 |   1.0352 | 1.06008 |
#  
#  ------------------------------
#  