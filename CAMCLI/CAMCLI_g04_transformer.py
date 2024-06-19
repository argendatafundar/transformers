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
rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'subsubsector': 'nivel3', 'valor_en_porcent': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   sector            29 non-null     object 
#   1   subsector         29 non-null     object 
#   2   subsubsector      29 non-null     object 
#   3   valor_en_porcent  29 non-null     float64
#  
#  |    | sector   | subsector                                      | subsubsector   |   valor_en_porcent |
#  |---:|:---------|:-----------------------------------------------|:---------------|-------------------:|
#  |  0 | Energía  | Industrias manufactureras y de la construcción | Hierro y acero |                7.2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'subsubsector': 'nivel3', 'valor_en_porcent': 'valor'})
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  29 non-null     object 
#   1   nivel2  29 non-null     object 
#   2   nivel3  29 non-null     object 
#   3   valor   29 non-null     float64
#  
#  |    | nivel1   | nivel2                                         | nivel3         |   valor |
#  |---:|:---------|:-----------------------------------------------|:---------------|--------:|
#  |  0 | Energía  | Industrias manufactureras y de la construcción | Hierro y acero |     7.2 |
#  
#  ------------------------------
#  