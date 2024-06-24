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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	rename_cols(map={'idha_subdimension': 'categoria'}),
	drop_col(col=['iso3', 'iso3_desc_fundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20880 entries, 0 to 20879
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               20880 non-null  object 
#   1   iso3_desc_fundar   20880 non-null  object 
#   2   anio               20880 non-null  int64  
#   3   idha_subdimension  20880 non-null  object 
#   4   valor              18425 non-null  float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio | idha_subdimension      |   valor |
#  |---:|:-------|:-------------------|-------:|:-----------------------|--------:|
#  |  0 | AFG    | Afganistán         |   1870 | Años de escolarización |     nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 120 entries, 1920 to 2039
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               120 non-null    object 
#   1   iso3_desc_fundar   120 non-null    object 
#   2   anio               120 non-null    int64  
#   3   idha_subdimension  120 non-null    object 
#   4   valor              120 non-null    float64
#  
#  |      | iso3   | iso3_desc_fundar   |   anio | idha_subdimension      |     valor |
#  |-----:|:-------|:-------------------|-------:|:-----------------------|----------:|
#  | 1920 | ARG    | Argentina          |   1870 | Años de escolarización | 0.0388517 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idha_subdimension': 'categoria'})
#  Index: 120 entries, 1920 to 2039
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              120 non-null    object 
#   1   iso3_desc_fundar  120 non-null    object 
#   2   anio              120 non-null    int64  
#   3   categoria         120 non-null    object 
#   4   valor             120 non-null    float64
#  
#  |      | iso3   | iso3_desc_fundar   |   anio | categoria              |     valor |
#  |-----:|:-------|:-------------------|-------:|:-----------------------|----------:|
#  | 1920 | ARG    | Argentina          |   1870 | Años de escolarización | 0.0388517 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'iso3_desc_fundar'], axis=1)
#  Index: 120 entries, 1920 to 2039
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       120 non-null    int64  
#   1   categoria  120 non-null    object 
#   2   valor      120 non-null    float64
#  
#  |      |   anio | categoria              |     valor |
#  |-----:|-------:|:-----------------------|----------:|
#  | 1920 |   1870 | Años de escolarización | 0.0388517 |
#  
#  ------------------------------
#  