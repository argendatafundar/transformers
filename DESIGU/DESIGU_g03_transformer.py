from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def remove_substring(df: DataFrame, col: str, substring: str) -> DataFrame:
    df[col] = df[col].str.replace(substring, "", regex=False)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'brecha': 'valor', 'trimestre': 'aniotrim'}),
	query(condition="aniotrim.str.endswith('-1')"),
	remove_substring(col='aniotrim', substring='-1')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   trimestre  84 non-null     object 
#   1   brecha     79 non-null     float64
#  
#  |    | trimestre   |   brecha |
#  |---:|:------------|---------:|
#  |  0 | 2003-3      |     53.9 |
#  
#  ------------------------------
#  
#  rename_cols(map={'brecha': 'valor', 'trimestre': 'aniotrim'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  84 non-null     object 
#   1   valor     79 non-null     float64
#  
#  |    | aniotrim   |   valor |
#  |---:|:-----------|--------:|
#  |  0 | 2003-3     |    53.9 |
#  
#  ------------------------------
#  
#  query(condition="aniotrim.str.endswith('-1')")
#  Index: 21 entries, 2 to 82
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  21 non-null     object 
#   1   valor     20 non-null     float64
#  
#  |    |   aniotrim |   valor |
#  |---:|-----------:|--------:|
#  |  2 |       2004 |    39.2 |
#  
#  ------------------------------
#  
#  remove_substring(col='aniotrim', substring='-1')
#  Index: 21 entries, 2 to 82
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  21 non-null     object 
#   1   valor     20 non-null     float64
#  
#  |    |   aniotrim |   valor |
#  |---:|-----------:|--------:|
#  |  2 |       2004 |    39.2 |
#  
#  ------------------------------
#  