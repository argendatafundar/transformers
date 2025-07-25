from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            20 non-null     int64  
#   1   vbp_ssi_2022    20 non-null     int64  
#   2   prop_ssi_total  19 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |   prop_ssi_total |
#  |---:|-------:|---------------:|-----------------:|
#  |  0 |   2003 |         348265 |              nan |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            20 non-null     int64  
#   1   vbp_ssi_2022    20 non-null     int64  
#   2   prop_ssi_total  19 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |   prop_ssi_total |
#  |---:|-------:|---------------:|-----------------:|
#  |  0 |   2003 |         348265 |              nan |
#  
#  ------------------------------
#  