from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	drop_col(col='reportingeconomy', axis=1),
	rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16314 entries, 0 to 16313
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    16314 non-null  int64  
#   1   iso3                    16314 non-null  object 
#   2   reportingeconomy        16314 non-null  object 
#   3   productsector_agregado  16314 non-null  object 
#   4   export_value_pc         16312 non-null  float64
#  
#  |    |   year | iso3   | reportingeconomy                       | productsector_agregado        |   export_value_pc |
#  |---:|-------:|:-------|:---------------------------------------|:------------------------------|------------------:|
#  |  0 |   2005 | ABW    | Aruba, the Netherlands with respect to | Otros servicios empresariales |           9.33435 |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 90 entries, 23 to 15652
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    90 non-null     int64  
#   1   iso3                    90 non-null     object 
#   2   reportingeconomy        90 non-null     object 
#   3   productsector_agregado  90 non-null     object 
#   4   export_value_pc         90 non-null     float64
#  
#  |    |   year | iso3   | reportingeconomy   | productsector_agregado        |   export_value_pc |
#  |---:|-------:|:-------|:-------------------|:------------------------------|------------------:|
#  | 23 |   2005 | ARG    | Argentina          | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 90 entries, 23 to 15652
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    90 non-null     int64  
#   1   reportingeconomy        90 non-null     object 
#   2   productsector_agregado  90 non-null     object 
#   3   export_value_pc         90 non-null     float64
#  
#  |    |   year | reportingeconomy   | productsector_agregado        |   export_value_pc |
#  |---:|-------:|:-------------------|:------------------------------|------------------:|
#  | 23 |   2005 | Argentina          | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  drop_col(col='reportingeconomy', axis=1)
#  Index: 90 entries, 23 to 15652
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   year                    90 non-null     int64  
#   1   productsector_agregado  90 non-null     object 
#   2   export_value_pc         90 non-null     float64
#  
#  |    |   year | productsector_agregado        |   export_value_pc |
#  |---:|-------:|:------------------------------|------------------:|
#  | 23 |   2005 | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor'})
#  Index: 90 entries, 23 to 15652
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       90 non-null     int64  
#   1   indicador  90 non-null     object 
#   2   valor      90 non-null     float64
#  
#  |    |   year | indicador                     |   valor |
#  |---:|-------:|:------------------------------|--------:|
#  | 23 |   2005 | Otros servicios empresariales | 24.7445 |
#  
#  ------------------------------
#  