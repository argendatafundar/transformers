from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'pbi_agro_pcons': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype
#  ---  ------          --------------  -----
#   0   anio            148 non-null    int64
#   1   pbi_agro_pcons  148 non-null    int64
#  
#  |    |   anio |   pbi_agro_pcons |
#  |---:|-------:|-----------------:|
#  |  0 |   1875 |              971 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pbi_agro_pcons': 'valor'})
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype
#  ---  ------  --------------  -----
#   0   anio    148 non-null    int64
#   1   valor   148 non-null    int64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1875 |   97100 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype
#  ---  ------  --------------  -----
#   0   anio    148 non-null    int64
#   1   valor   148 non-null    int64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1875 |   97100 |
#  
#  ------------------------------
#  