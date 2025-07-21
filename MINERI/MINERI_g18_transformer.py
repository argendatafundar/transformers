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
	rename_cols(map={'rama_vendedora': 'categoria', 'porcentaje_compra_sector_minero': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 2 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   rama_vendedora                   15 non-null     object 
#   1   porcentaje_compra_sector_minero  15 non-null     float64
#  
#  |    | rama_vendedora   |   porcentaje_compra_sector_minero |
#  |---:|:-----------------|----------------------------------:|
#  |  0 | Industria        |                                24 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama_vendedora': 'categoria', 'porcentaje_compra_sector_minero': 'valor'})
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   valor      15 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Industria   |      24 |
#  
#  ------------------------------
#  