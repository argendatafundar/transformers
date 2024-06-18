from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(nivel='categoria', sector='indicador', prop_educ='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   nivel      21 non-null     object 
#   1   sector     21 non-null     object 
#   2   prop_educ  21 non-null     float64
#  
#  |    | nivel                  | sector   |   prop_educ |
#  |---:|:-----------------------|:---------|------------:|
#  |  0 | a. Primario incompleto | SBC      |  0.00194999 |
#  
#  ------------------------------
#  
#  rename_columns(nivel='categoria', sector='indicador', prop_educ='valor')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  21 non-null     object 
#   1   indicador  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | categoria              | indicador   |      valor |
#  |---:|:-----------------------|:------------|-----------:|
#  |  0 | a. Primario incompleto | SBC         | 0.00194999 |
#  
#  ------------------------------
#  