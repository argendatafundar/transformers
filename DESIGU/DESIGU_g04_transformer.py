from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'gini': 'valor', 'code': 'geocodigo'}),
	drop_col(col='pais', axis=1),
	drop_na(cols='valor'),
	sort_values(how='ascending', by=['valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   code    162 non-null    object 
#   1   pais    162 non-null    object 
#   2   gini    153 non-null    float64
#  
#  |    | code   | pais    |   gini |
#  |---:|:-------|:--------|-------:|
#  |  0 | ISL    | Iceland |   24.3 |
#  
#  ------------------------------
#  
#  rename_cols(map={'gini': 'valor', 'code': 'geocodigo'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   pais       162 non-null    object 
#   2   valor      153 non-null    float64
#  
#  |    | geocodigo   | pais    |   valor |
#  |---:|:------------|:--------|--------:|
#  |  0 | ISL         | Iceland |    24.3 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   valor      153 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | ISL         |    24.3 |
#  
#  ------------------------------
#  
#  drop_na(cols='valor')
#  Index: 153 entries, 0 to 161
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  153 non-null    object 
#   1   valor      153 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | ISL         |    24.3 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['valor'])
#  RangeIndex: 153 entries, 0 to 152
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  153 non-null    object 
#   1   valor      153 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | ISL         |    24.3 |
#  
#  ------------------------------
#  