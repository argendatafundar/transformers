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
rename_cols(map={'tipo_energia': 'serie', 'mineral_critico': 'indicador', 'mineral_utilizado': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   tipo_energia       60 non-null     object 
#   1   mineral_critico    60 non-null     object 
#   2   mineral_utilizado  60 non-null     float64
#  
#  |    | tipo_energia    | mineral_critico   |   mineral_utilizado |
#  |---:|:----------------|:------------------|--------------------:|
#  |  0 | eolica_offshore | cobre             |                8000 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'serie', 'mineral_critico': 'indicador', 'mineral_utilizado': 'valor'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   serie      60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | serie           | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | cobre       |    8000 |
#  
#  ------------------------------
#  