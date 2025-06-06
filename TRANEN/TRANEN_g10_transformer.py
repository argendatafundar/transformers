from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='fuente_energia == "Biocombustibles"'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['valor_en_twh', 'tipo_energia', 'indicador'], axis=1),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 52923 entries, 0 to 52922
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            52923 non-null  object 
#   1   anio            52923 non-null  int64  
#   2   fuente_energia  52923 non-null  object 
#   3   tipo_energia    52923 non-null  object 
#   4   valor_en_twh    41782 non-null  float64
#   5   porcentaje      52923 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  query(condition='fuente_energia == "Biocombustibles"')
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            6490 non-null   object 
#   1   anio            6490 non-null   int64  
#   2   fuente_energia  6490 non-null   object 
#   3   tipo_energia    6490 non-null   object 
#   4   valor_en_twh    1885 non-null   float64
#   5   porcentaje      6490 non-null   float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            6490 non-null   object 
#   1   anio            6490 non-null   int64  
#   2   fuente_energia  6490 non-null   object 
#   3   tipo_energia    6490 non-null   object 
#   4   valor_en_twh    1885 non-null   float64
#   5   porcentaje      6490 non-null   float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'})
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     6490 non-null   object 
#   1   anio          6490 non-null   int64  
#   2   indicador     6490 non-null   object 
#   3   tipo_energia  6490 non-null   object 
#   4   valor_en_twh  1885 non-null   float64
#   5   valor         6490 non-null   float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor_en_twh |   valor |
#  |---:|:------------|-------:|:----------------|:---------------|---------------:|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |            nan |       0 |
#  
#  ------------------------------
#  
#  drop_col(col=['valor_en_twh', 'tipo_energia', 'indicador'], axis=1)
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6490 non-null   object 
#   1   anio       6490 non-null   int64  
#   2   valor      6490 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   1965 |       0 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6490 non-null   object 
#   1   anio       6490 non-null   int64  
#   2   valor      6490 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   1965 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  Index: 6490 entries, 0 to 52804
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6490 non-null   object 
#   1   anio       6490 non-null   int64  
#   2   valor      6490 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   1965 |       0 |
#  
#  ------------------------------
#  