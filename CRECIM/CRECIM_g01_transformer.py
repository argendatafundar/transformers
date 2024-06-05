from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais_nombre', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype 
#  ---  ------          --------------  ----- 
#   0   iso3            20 non-null     object
#   1   pais_nombre     20 non-null     object
#   2   ranking_pib     20 non-null     int64 
#   3   ranking_pib_pc  20 non-null     int64 
#  
#  |    | iso3   | pais_nombre   |   ranking_pib |   ranking_pib_pc |
#  |---:|:-------|:--------------|--------------:|-----------------:|
#  |  0 | ARG    | Argentina     |            26 |               84 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype 
#  ---  ------          --------------  ----- 
#   0   iso3            20 non-null     object
#   1   ranking_pib     20 non-null     int64 
#   2   ranking_pib_pc  20 non-null     int64 
#  
#  |    | iso3   |   ranking_pib |   ranking_pib_pc |
#  |---:|:-------|--------------:|-----------------:|
#  |  0 | ARG    |            26 |               84 |
#  
#  ------------------------------
#  