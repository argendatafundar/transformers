from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def datetime_to_year(df: DataFrame, col: str):
    df[col] = pd.to_datetime(df[col]).dt.year
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
datetime_to_year(col='fecha'),
	rename_cols(map={'variable': 'serie'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4736 entries, 0 to 4735
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   fecha     4736 non-null   object 
#   1   variable  4736 non-null   object 
#   2   valor     4736 non-null   float64
#  
#  |    | fecha      | variable   |   valor |
#  |---:|:-----------|:-----------|--------:|
#  |  0 | 1960-01-01 | oro        |   35.27 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='fecha')
#  RangeIndex: 4736 entries, 0 to 4735
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   fecha     4736 non-null   int32  
#   1   variable  4736 non-null   object 
#   2   valor     4736 non-null   float64
#  
#  |    |   fecha | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |    1960 | oro        |   35.27 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'serie'})
#  RangeIndex: 4736 entries, 0 to 4735
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   fecha   4736 non-null   int32  
#   1   serie   4736 non-null   object 
#   2   valor   4736 non-null   float64
#  
#  |    |   fecha | serie   |   valor |
#  |---:|--------:|:--------|--------:|
#  |  0 |    1960 | oro     |   35.27 |
#  
#  ------------------------------
#  