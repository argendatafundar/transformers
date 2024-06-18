from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'}),
	drop_col(col='pais', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3c              756 non-null    object 
#   1   anio               756 non-null    int64  
#   2   va_agro_sobre_pbi  631 non-null    float64
#   3   pais               756 non-null    object 
#  
#  |    | iso3c   |   anio |   va_agro_sobre_pbi | pais                   |
#  |---:|:--------|-------:|--------------------:|:-----------------------|
#  |  0 | HIC     |   2022 |                 nan | Países de ingreso alto |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3c': 'geocodigo', 'va_agro_sobre_pbi': 'valor'})
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  756 non-null    object 
#   1   anio       756 non-null    int64  
#   2   valor      631 non-null    float64
#   3   pais       756 non-null    object 
#  
#  |    | geocodigo   |   anio |   valor | pais                   |
#  |---:|:------------|-------:|--------:|:-----------------------|
#  |  0 | HIC         |   2022 |     nan | Países de ingreso alto |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  756 non-null    object 
#   1   anio       756 non-null    int64  
#   2   valor      631 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | HIC         |   2022 |     nan |
#  
#  ------------------------------
#  