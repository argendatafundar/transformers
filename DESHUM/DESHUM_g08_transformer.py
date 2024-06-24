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
	rename_cols(map={'idh_tipo': 'categoria'}),
	drop_col(col=['iso3', 'country'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 40788 entries, 0 to 40787
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   iso3      40788 non-null  object 
#   1   country   40788 non-null  object 
#   2   anio      40788 non-null  int64  
#   3   idh_tipo  40788 non-null  object 
#   4   valor     38647 non-null  float64
#  
#  |    | iso3   | country     |   anio | idh_tipo                             |    valor |
#  |---:|:-------|:------------|-------:|:-------------------------------------|---------:|
#  |  0 | AFG    | Afghanistan |   1990 | IDH años esperados de escolarización | 0.163137 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 198 entries, 990 to 1187
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   iso3      198 non-null    object 
#   1   country   198 non-null    object 
#   2   anio      198 non-null    int64  
#   3   idh_tipo  198 non-null    object 
#   4   valor     198 non-null    float64
#  
#  |     | iso3   | country   |   anio | idh_tipo                             |    valor |
#  |----:|:-------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG    | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh_tipo': 'categoria'})
#  Index: 198 entries, 990 to 1187
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       198 non-null    object 
#   1   country    198 non-null    object 
#   2   anio       198 non-null    int64  
#   3   categoria  198 non-null    object 
#   4   valor      198 non-null    float64
#  
#  |     | iso3   | country   |   anio | categoria                            |    valor |
#  |----:|:-------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG    | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3', 'country'], axis=1)
#  Index: 198 entries, 990 to 1187
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |     |   anio | categoria                            |    valor |
#  |----:|-------:|:-------------------------------------|---------:|
#  | 990 |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  