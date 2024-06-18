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
rename_columns(iso3='geocodigo', inflacion='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3168 entries, 0 to 3167
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3168 non-null   int64  
#   1   inflacion  3168 non-null   float64
#   2   iso3       3168 non-null   object 
#  
#  |    |   anio |   inflacion | iso3   |
#  |---:|-------:|------------:|:-------|
#  |  0 |   2000 |   -0.733699 | ARG    |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', inflacion='valor')
#  RangeIndex: 3168 entries, 0 to 3167
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3168 non-null   int64  
#   1   valor      3168 non-null   float64
#   2   geocodigo  3168 non-null   object 
#  
#  |    |   anio |     valor | geocodigo   |
#  |---:|-------:|----------:|:------------|
#  |  0 |   2000 | -0.733699 | ARG         |
#  
#  ------------------------------
#  