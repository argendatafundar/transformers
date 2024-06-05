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
rename_cols(map={'fecha_estimada': 'anio', 'co2_ppm': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1096 entries, 0 to 1095
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   fecha_estimada  1096 non-null   int64  
#   1   co2_ppm         1096 non-null   float64
#  
#  |    |   fecha_estimada |   co2_ppm |
#  |---:|-----------------:|----------:|
#  |  0 |             -137 |     280.4 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha_estimada': 'anio', 'co2_ppm': 'valor'})
#  RangeIndex: 1096 entries, 0 to 1095
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    1096 non-null   int64  
#   1   valor   1096 non-null   float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   -137 |   280.4 |
#  
#  ------------------------------
#  