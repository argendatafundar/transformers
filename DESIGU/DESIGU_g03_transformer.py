from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'brecha': 'valor', 'trimestre': 'aniotrim'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 82 entries, 0 to 81
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   trimestre  82 non-null     object 
#   1   brecha     77 non-null     float64
#  
#  |    | trimestre   |   brecha |
#  |---:|:------------|---------:|
#  |  0 | 2003-3      |     53.9 |
#  
#  ------------------------------
#  
#  rename_cols(map={'brecha': 'valor', 'trimestre': 'aniotrim'})
#  RangeIndex: 82 entries, 0 to 81
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  82 non-null     object 
#   1   valor     77 non-null     float64
#  
#  |    | aniotrim   |   valor |
#  |---:|:-----------|--------:|
#  |  0 | 2003-3     |    53.9 |
#  
#  ------------------------------
#  