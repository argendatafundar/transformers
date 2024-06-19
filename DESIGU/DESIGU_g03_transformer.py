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
rename_cols(map={'brecha': 'valor', 'trimestre': 'anio'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   trimestre  80 non-null     object 
#   1   brecha     75 non-null     float64
#  
#  |    | trimestre   |   brecha |
#  |---:|:------------|---------:|
#  |  0 | 2003-3      |     53.9 |
#  
#  ------------------------------
#  
#  rename_cols(map={'brecha': 'valor', 'trimestre': 'anio'})
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    80 non-null     object 
#   1   valor   75 non-null     float64
#  
#  |    | anio   |   valor |
#  |---:|:-------|--------:|
#  |  0 | 2003-3 |    53.9 |
#  
#  ------------------------------
#  