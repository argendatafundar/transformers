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
rename_columns(iso3='geocodigo', prop_expo='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   prop_expo  3194 non-null   float64
#  
#  |    | iso3   |   anio |   prop_expo |
#  |---:|:-------|-------:|------------:|
#  |  0 | AFG    |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', prop_expo='valor')
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   valor      3194 non-null   float64
#  
#  |    | geocodigo   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | AFG         |   2008 | 0.000248476 |
#  
#  ------------------------------
#  