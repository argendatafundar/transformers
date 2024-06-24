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
rename_cols(map={'iso3': 'geocodigo', 'puestos_per_capita': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12810 entries, 0 to 12809
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                12810 non-null  object 
#   1   anio                12810 non-null  int64  
#   2   puestos_per_capita  9529 non-null   float64
#  
#  |    | iso3   |   anio |   puestos_per_capita |
#  |---:|:-------|-------:|---------------------:|
#  |  0 | ABW    |   1950 |                  nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'puestos_per_capita': 'valor'})
#  RangeIndex: 12810 entries, 0 to 12809
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  12810 non-null  object 
#   1   anio       12810 non-null  int64  
#   2   valor      9529 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1950 |     nan |
#  
#  ------------------------------
#  