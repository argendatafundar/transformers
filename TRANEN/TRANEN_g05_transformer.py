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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
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
query(condition='tipo_energia != "Total"'),
	replace_value(col='fuente_energia', curr_value='Eolica', new_value='Eólica'),
	replace_value(col='fuente_energia', curr_value='Carbon', new_value='Carbón'),
	replace_value(col='fuente_energia', curr_value='Petroleo', new_value='Petróleo'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['valor_en_twh', 'tipo_energia'], axis=1),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo', 'indicador'])
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
#  query(condition='tipo_energia != "Total"')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            46433 non-null  object 
#   1   anio            46433 non-null  int64  
#   2   fuente_energia  46433 non-null  object 
#   3   tipo_energia    46433 non-null  object 
#   4   valor_en_twh    35292 non-null  float64
#   5   porcentaje      46433 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', curr_value='Eolica', new_value='Eólica')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            46433 non-null  object 
#   1   anio            46433 non-null  int64  
#   2   fuente_energia  46433 non-null  object 
#   3   tipo_energia    46433 non-null  object 
#   4   valor_en_twh    35292 non-null  float64
#   5   porcentaje      46433 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', curr_value='Carbon', new_value='Carbón')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            46433 non-null  object 
#   1   anio            46433 non-null  int64  
#   2   fuente_energia  46433 non-null  object 
#   3   tipo_energia    46433 non-null  object 
#   4   valor_en_twh    35292 non-null  float64
#   5   porcentaje      46433 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='fuente_energia', curr_value='Petroleo', new_value='Petróleo')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            46433 non-null  object 
#   1   anio            46433 non-null  int64  
#   2   fuente_energia  46433 non-null  object 
#   3   tipo_energia    46433 non-null  object 
#   4   valor_en_twh    35292 non-null  float64
#   5   porcentaje      46433 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            46433 non-null  object 
#   1   anio            46433 non-null  int64  
#   2   fuente_energia  46433 non-null  object 
#   3   tipo_energia    46433 non-null  object 
#   4   valor_en_twh    35292 non-null  float64
#   5   porcentaje      46433 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'iso3': 'geocodigo'})
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor_en_twh  35292 non-null  float64
#   5   valor         46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor_en_twh |   valor |
#  |---:|:------------|-------:|:----------------|:---------------|---------------:|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |            nan |       0 |
#  
#  ------------------------------
#  
#  drop_col(col=['valor_en_twh', 'tipo_energia'], axis=1)
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  46433 non-null  object 
#   1   anio       46433 non-null  int64  
#   2   indicador  46433 non-null  object 
#   3   valor      46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |       0 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  46433 non-null  object 
#   1   anio       46433 non-null  int64  
#   2   indicador  46433 non-null  object 
#   3   valor      46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo', 'indicador'])
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  46433 non-null  object 
#   1   anio       46433 non-null  int64  
#   2   indicador  46433 non-null  object 
#   3   valor      46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |       0 |
#  
#  ------------------------------
#  