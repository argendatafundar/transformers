from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

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
drop_col(col='continente_fundar', axis=1),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'pib_per_capita': 'valor'}),
	drop_na(cols=['valor']),
	replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD'),
	replace_value(col='geocodigo', curr_value='YUG', new_value='SER'),
	replace_value(col='geocodigo', curr_value='SUN', new_value='SVU'),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition='anio >= 1900')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               21618 non-null  object 
#   1   pais_nombre        21618 non-null  object 
#   2   continente_fundar  21366 non-null  object 
#   3   es_agregacion      21618 non-null  int64  
#   4   anio               21618 non-null  int64  
#   5   pib_per_capita     21586 non-null  float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|-----------------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            21618 non-null  object 
#   1   pais_nombre     21618 non-null  object 
#   2   es_agregacion   21618 non-null  int64  
#   3   anio            21618 non-null  int64  
#   4   pib_per_capita  21586 non-null  float64
#  
#  |    | iso3   | pais_nombre   |   es_agregacion |   anio |   pib_per_capita |
#  |---:|:-------|:--------------|----------------:|-------:|-----------------:|
#  |  0 | AFG    | Afganistán    |               0 |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            21618 non-null  object 
#   1   pais_nombre     21618 non-null  object 
#   2   anio            21618 non-null  int64  
#   3   pib_per_capita  21586 non-null  float64
#  
#  |    | iso3   | pais_nombre   |   anio |   pib_per_capita |
#  |---:|:-------|:--------------|-------:|-----------------:|
#  |  0 | AFG    | Afganistán    |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            21618 non-null  object 
#   1   anio            21618 non-null  int64  
#   2   pib_per_capita  21586 non-null  float64
#  
#  |    | iso3   |   anio |   pib_per_capita |
#  |---:|:-------|-------:|-----------------:|
#  |  0 | AFG    |   1950 |             1156 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'pib_per_capita': 'valor'})
#  RangeIndex: 21618 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21618 non-null  object 
#   1   anio       21618 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |    1156 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 21586 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21586 non-null  object 
#   1   anio       21586 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |    1156 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD')
#  Index: 21586 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21586 non-null  object 
#   1   anio       21586 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |    1156 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='YUG', new_value='SER')
#  Index: 21586 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21586 non-null  object 
#   1   anio       21586 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |    1156 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='SUN', new_value='SVU')
#  Index: 21586 entries, 0 to 21617
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21586 non-null  object 
#   1   anio       21586 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1950 |    1156 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 21586 entries, 0 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  21586 non-null  object 
#   1   anio       21586 non-null  int64  
#   2   valor      21586 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | BEL         |      1 |     956 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1900')
#  Index: 14886 entries, 6700 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  14886 non-null  object 
#   1   anio       14886 non-null  int64  
#   2   valor      14886 non-null  float64
#  
#  |      | geocodigo   |   anio |   valor |
#  |-----:|:------------|-------:|--------:|
#  | 6700 | ALB         |   1900 |    1092 |
#  
#  ------------------------------
#  