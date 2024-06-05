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
rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             825 non-null    int64  
#   1   sector           825 non-null    object 
#   2   valor_en_ggco2e  825 non-null    float64
#  
#  |    |   anio | sector   |   valor_en_ggco2e |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1850 | AGSyOUT  |         4.896e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'})
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       825 non-null    int64  
#   1   indicador  825 non-null    object 
#   2   valor      825 non-null    float64
#  
#  |    |   anio | indicador   |     valor |
#  |---:|-------:|:------------|----------:|
#  |  0 |   1850 | AGSyOUT     | 4.896e+06 |
#  
#  ------------------------------
#  