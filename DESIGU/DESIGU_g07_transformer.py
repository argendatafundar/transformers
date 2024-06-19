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
rename_cols(map={'cohorte': 'categoria', 'indicadormovilidad': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5 entries, 0 to 4
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   cohorte             5 non-null      object 
#   1   indicadormovilidad  5 non-null      float64
#  
#  |    | cohorte   |   indicadormovilidad |
#  |---:|:----------|---------------------:|
#  |  0 | 1940-1949 |                0.498 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cohorte': 'categoria', 'indicadormovilidad': 'valor'})
#  RangeIndex: 5 entries, 0 to 4
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  5 non-null      object 
#   1   valor      5 non-null      float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | 1940-1949   |   0.498 |
#  
#  ------------------------------
#  