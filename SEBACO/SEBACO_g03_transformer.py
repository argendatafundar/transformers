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
rename_cols(map={'vab': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    4 non-null      int64  
#   1   vab     4 non-null      float64
#  
#  |    |   anio |    vab |
#  |---:|-------:|-------:|
#  |  0 |   2004 | 0.0046 |
#  
#  ------------------------------
#  
#  rename_cols(map={'vab': 'valor'})
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    4 non-null      int64  
#   1   valor   4 non-null      float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   2004 |  0.0046 |
#  
#  ------------------------------
#  