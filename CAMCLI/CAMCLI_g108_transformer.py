from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'valor_en_mtco2e': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 2 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             29 non-null     int64  
#   1   valor_en_mtco2e  29 non-null     float64
#  
#  |    |   anio |   valor_en_mtco2e |
#  |---:|-------:|------------------:|
#  |  0 |   1990 |            263.54 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor'})
#  RangeIndex: 29 entries, 0 to 28
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    29 non-null     int64  
#   1   valor   29 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1990 |  263.54 |
#  
#  ------------------------------
#  