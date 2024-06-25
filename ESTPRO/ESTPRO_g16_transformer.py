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
rename_cols(map={'iso3': 'geocodigo', 'pib_por_ocupado': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               9529 non-null   object 
#   1   pais_nombre        9529 non-null   object 
#   2   continente_fundar  9529 non-null   object 
#   3   anio               9529 non-null   int64  
#   4   pib_por_ocupado    9529 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   pib_por_ocupado |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|------------------:|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1991 |           62628.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'pib_por_ocupado': 'valor'})
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          9529 non-null   object 
#   1   pais_nombre        9529 non-null   object 
#   2   continente_fundar  9529 non-null   object 
#   3   anio               9529 non-null   int64  
#   4   valor              9529 non-null   float64
#  
#  |    | geocodigo   | pais_nombre   | continente_fundar                      |   anio |   valor |
#  |---:|:------------|:--------------|:---------------------------------------|-------:|--------:|
#  |  0 | ABW         | Aruba         | América del Norte, Central y el Caribe |   1991 | 62628.8 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar'], axis=1)
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9529 non-null   object 
#   1   anio       9529 non-null   int64  
#   2   valor      9529 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1991 | 62628.8 |
#  
#  ------------------------------
#  