from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'pib_por_ocupado': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    9529 non-null   object 
#   1   geonombreFundar    9529 non-null   object 
#   2   continente_fundar  9529 non-null   object 
#   3   anio               9529 non-null   int64  
#   4   pib_por_ocupado    9529 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar                      |   anio |   pib_por_ocupado |
#  |---:|:------------------|:------------------|:---------------------------------------|-------:|------------------:|
#  |  0 | ABW               | Aruba             | América del Norte, Central y el Caribe |   1991 |           62628.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pib_por_ocupado': 'valor'})
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    9529 non-null   object 
#   1   geonombreFundar    9529 non-null   object 
#   2   continente_fundar  9529 non-null   object 
#   3   anio               9529 non-null   int64  
#   4   valor              9529 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar                      |   anio |   valor |
#  |---:|:------------------|:------------------|:---------------------------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             | América del Norte, Central y el Caribe |   1991 | 62628.8 |
#  
#  ------------------------------
#  