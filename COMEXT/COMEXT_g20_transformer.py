from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict):
    mapcol = colname+'_map'
    df[mapcol] = df[colname].map(precedence)
    df.sort_values(by=['anio', mapcol], inplace=True)
    df.drop(mapcol, inplace=True, axis=1)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition="geocodigoFundar == 'ARG'"),
	drop_col(col='reportingeconomy', axis=1),
	rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor', 'year': 'anio'}),
	sort_values_by_comparison(colname='indicador', precedence={'Viajes': 0, 'Otros servicios empresariales': 1, 'Serv. de telecom., informática e información': 2, 'Transportes': 3, 'Resto': 4})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16314 entries, 0 to 16313
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         16314 non-null  object 
#   1   geonombreFundar         16314 non-null  object 
#   2   year                    16314 non-null  int64  
#   3   reportingeconomy        16314 non-null  object 
#   4   productsector_agregado  16314 non-null  object 
#   5   export_value_pc         16312 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | reportingeconomy                       | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:---------------------------------------|:------------------------------|------------------:|
#  |  0 | ABW               | Aruba             |   2005 | Aruba, the Netherlands with respect to | Otros servicios empresariales |           9.33435 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 90 entries, 23 to 15652
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         90 non-null     object 
#   1   geonombreFundar         90 non-null     object 
#   2   year                    90 non-null     int64  
#   3   reportingeconomy        90 non-null     object 
#   4   productsector_agregado  90 non-null     object 
#   5   export_value_pc         90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | reportingeconomy   | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------|:------------------------------|------------------:|
#  | 23 | ARG               | Argentina         |   2005 | Argentina          | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  drop_col(col='reportingeconomy', axis=1)
#  Index: 90 entries, 23 to 15652
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         90 non-null     object 
#   1   geonombreFundar         90 non-null     object 
#   2   year                    90 non-null     int64  
#   3   productsector_agregado  90 non-null     object 
#   4   export_value_pc         90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:------------------------------|------------------:|
#  | 23 | ARG               | Argentina         |   2005 | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor', 'year': 'anio'})
#  Index: 90 entries, 27 to 15650
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  90 non-null     object 
#   1   geonombreFundar  90 non-null     object 
#   2   anio             90 non-null     int64  
#   3   indicador        90 non-null     object 
#   4   valor            90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  | 27 | ARG               | Argentina         |   2005 | Viajes      | 42.2577 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Viajes': 0, 'Otros servicios empresariales': 1, 'Serv. de telecom., informática e información': 2, 'Transportes': 3, 'Resto': 4})
#  Index: 90 entries, 27 to 15650
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  90 non-null     object 
#   1   geonombreFundar  90 non-null     object 
#   2   anio             90 non-null     int64  
#   3   indicador        90 non-null     object 
#   4   valor            90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  | 27 | ARG               | Argentina         |   2005 | Viajes      | 42.2577 |
#  
#  ------------------------------
#  