from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
	query(condition='geocodigoFundar == "ARG"'),
	rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector_bp_name': 'categoria', 'import_value_pc': 'valor'}),
	drop_col(col='country_name_abbreviation', axis=1),
	drop_col(col='sector_bp', axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 2930 entries, 0 to 2929
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            2930 non-null   object 
#   1   geonombreFundar            2930 non-null   object 
#   2   anio                       2930 non-null   int64  
#   3   country_name_abbreviation  2930 non-null   object 
#   4   sector_bp                  2930 non-null   int64  
#   5   sector_bp_name             2930 non-null   object 
#   6   import_value_pc            2912 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | sector_bp_name       |   import_value_pc |
#  |---:|:------------------|:------------------|-------:|:----------------------------|------------:|:---------------------|------------------:|
#  |  0 | BGD               | Bangladesh        |   2020 | Bangladesh                  |           2 | Alimentos procesados |           2.64054 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            13 non-null     object 
#   1   geonombreFundar            13 non-null     object 
#   2   anio                       13 non-null     int64  
#   3   country_name_abbreviation  13 non-null     object 
#   4   sector_bp                  13 non-null     int64  
#   5   sector_bp_name             13 non-null     object 
#   6   import_value_pc            13 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | sector_bp_name   |   import_value_pc |
#  |----:|:------------------|:------------------|-------:|:----------------------------|------------:|:-----------------|------------------:|
#  | 477 | ARG               | Argentina         |   2020 | Argentina                   |           6 | Cueros y pieles  |          0.242406 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector_bp_name': 'categoria', 'import_value_pc': 'valor'})
#  Index: 13 entries, 477 to 2607
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigo                  13 non-null     object 
#   1   geonombreFundar            13 non-null     object 
#   2   anio                       13 non-null     int64  
#   3   country_name_abbreviation  13 non-null     object 
#   4   sector_bp                  13 non-null     int64  
#   5   categoria                  13 non-null     object 
#   6   valor                      13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | categoria       |    valor |
#  |----:|:------------|:------------------|-------:|:----------------------------|------------:|:----------------|---------:|
#  | 477 | ARG         | Argentina         |   2020 | Argentina                   |           6 | Cueros y pieles | 0.242406 |
#  
#  ------------------------------
#  
#  drop_col(col='country_name_abbreviation', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   sector_bp        13 non-null     int64  
#   4   categoria        13 non-null     object 
#   5   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio |   sector_bp | categoria       |    valor |
#  |----:|:------------|:------------------|-------:|------------:|:----------------|---------:|
#  | 477 | ARG         | Argentina         |   2020 |           6 | Cueros y pieles | 0.242406 |
#  
#  ------------------------------
#  
#  drop_col(col='sector_bp', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |    valor |
#  |----:|:------------|:------------------|-------:|:----------------|---------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.242406 |
#  
#  ------------------------------
#  