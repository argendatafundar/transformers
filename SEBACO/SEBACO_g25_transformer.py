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
	rename_cols(map={'impo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1266 entries, 0 to 1265
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    1266 non-null   int64  
#   1   iso3    1266 non-null   object 
#   2   impo    1266 non-null   float64
#  
#  |    |   anio | iso3   |   impo |
#  |---:|-------:|:-------|-------:|
#  |  0 |   2015 | NAM    |      0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'indicador'})
#  RangeIndex: 1266 entries, 0 to 1265
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1266 non-null   int64  
#   1   indicador  1266 non-null   object 
#   2   impo       1266 non-null   float64
#  
#  |    |   anio | indicador   |   impo |
#  |---:|-------:|:------------|-------:|
#  |  0 |   2015 | NAM         |      0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'impo': 'valor'})
#  RangeIndex: 1266 entries, 0 to 1265
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1266 non-null   int64  
#   1   indicador  1266 non-null   object 
#   2   valor      1266 non-null   float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2015 | NAM         |       0 |
#  
#  ------------------------------
#  