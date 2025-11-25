from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'valor_en_mtco2e': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   sector           132 non-null    object 
#   4   valor_en_mtco2e  132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | sector   |   valor_en_mtco2e |
#  |---:|-------:|:------------------|:------------------|:---------|------------------:|
#  |  0 |   1990 | Argentina         | ARG               | AGSyOUT  |           151.182 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor'})
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   sector           132 non-null    object 
#   4   valor            132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | sector   |   valor |
#  |---:|-------:|:------------------|:------------------|:---------|--------:|
#  |  0 |   1990 | Argentina         | ARG               | AGSyOUT  | 151.182 |
#  
#  ------------------------------
#  