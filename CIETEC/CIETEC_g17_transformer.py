from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     33 non-null     int64  
#   1   i_d_pib  27 non-null     float64
#   2   act_pib  33 non-null     float64
#  
#  |    |   anio |   i_d_pib |   act_pib |
#  |---:|-------:|----------:|----------:|
#  |  0 |   1990 |       nan |      0.33 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     33 non-null     int64  
#   1   i_d_pib  27 non-null     float64
#   2   act_pib  33 non-null     float64
#  
#  |    |   anio |   i_d_pib |   act_pib |
#  |---:|-------:|----------:|----------:|
#  |  0 |   1990 |       nan |      0.33 |
#  
#  ------------------------------
#  