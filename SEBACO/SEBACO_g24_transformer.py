from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'indicador'}),
	rename_cols(map={'expo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1351 entries, 0 to 1350
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    1351 non-null   int64  
#   1   iso3    1351 non-null   object 
#   2   expo    1351 non-null   float64
#  
#  |    |   anio | iso3   |   expo |
#  |---:|-------:|:-------|-------:|
#  |  0 |   2017 | NAM    |      0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'indicador'})
#  RangeIndex: 1351 entries, 0 to 1350
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1351 non-null   int64  
#   1   indicador  1351 non-null   object 
#   2   expo       1351 non-null   float64
#  
#  |    |   anio | indicador   |   expo |
#  |---:|-------:|:------------|-------:|
#  |  0 |   2017 | NAM         |      0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'expo': 'valor'})
#  RangeIndex: 1351 entries, 0 to 1350
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1351 non-null   int64  
#   1   indicador  1351 non-null   object 
#   2   valor      1351 non-null   float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2017 | NAM         |       0 |
#  
#  ------------------------------
#  