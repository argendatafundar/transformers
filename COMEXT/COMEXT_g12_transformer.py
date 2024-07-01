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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	rename_cols(map={'iso3': 'geocodigo', 'sector_bp_name': 'categoria', 'export_value_pc': 'valor'}),
	drop_col(col='country_name_abbreviation', axis=1),
	drop_col(col='sector_bp', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2930 entries, 0 to 2929
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       2930 non-null   int64  
#   1   iso3                       2930 non-null   object 
#   2   country_name_abbreviation  2930 non-null   object 
#   3   sector_bp                  2930 non-null   int64  
#   4   sector_bp_name             2930 non-null   object 
#   5   export_value_pc            2930 non-null   float64
#  
#  |    |   anio | iso3   | country_name_abbreviation   |   sector_bp | sector_bp_name       |   export_value_pc |
#  |---:|-------:|:-------|:----------------------------|------------:|:---------------------|------------------:|
#  |  0 |   2020 | BGD    | Bangladesh                  |           2 | Alimentos procesados |          0.726726 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       13 non-null     int64  
#   1   iso3                       13 non-null     object 
#   2   country_name_abbreviation  13 non-null     object 
#   3   sector_bp                  13 non-null     int64  
#   4   sector_bp_name             13 non-null     object 
#   5   export_value_pc            13 non-null     float64
#  
#  |     |   anio | iso3   | country_name_abbreviation   |   sector_bp | sector_bp_name   |   export_value_pc |
#  |----:|-------:|:-------|:----------------------------|------------:|:-----------------|------------------:|
#  | 477 |   2020 | ARG    | Argentina                   |           6 | Cueros y pieles  |           0.97719 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'sector_bp_name': 'categoria', 'export_value_pc': 'valor'})
#  Index: 13 entries, 477 to 2607
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       13 non-null     int64  
#   1   geocodigo                  13 non-null     object 
#   2   country_name_abbreviation  13 non-null     object 
#   3   sector_bp                  13 non-null     int64  
#   4   categoria                  13 non-null     object 
#   5   valor                      13 non-null     float64
#  
#  |     |   anio | geocodigo   | country_name_abbreviation   |   sector_bp | categoria       |   valor |
#  |----:|-------:|:------------|:----------------------------|------------:|:----------------|--------:|
#  | 477 |   2020 | ARG         | Argentina                   |           6 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  drop_col(col='country_name_abbreviation', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       13 non-null     int64  
#   1   geocodigo  13 non-null     object 
#   2   sector_bp  13 non-null     int64  
#   3   categoria  13 non-null     object 
#   4   valor      13 non-null     float64
#  
#  |     |   anio | geocodigo   |   sector_bp | categoria       |   valor |
#  |----:|-------:|:------------|------------:|:----------------|--------:|
#  | 477 |   2020 | ARG         |           6 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  drop_col(col='sector_bp', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       13 non-null     int64  
#   1   geocodigo  13 non-null     object 
#   2   categoria  13 non-null     object 
#   3   valor      13 non-null     float64
#  
#  |     |   anio | geocodigo   | categoria       |   valor |
#  |----:|-------:|:------------|:----------------|--------:|
#  | 477 |   2020 | ARG         | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  