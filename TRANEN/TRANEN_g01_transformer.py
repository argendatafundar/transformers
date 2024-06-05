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
rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          825 non-null    int64  
#   1   tipo_energia  825 non-null    object 
#   2   valor_en_twh  825 non-null    float64
#   3   porcentaje    825 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        825 non-null    int64  
#   1   indicador   825 non-null    object 
#   2   valor       825 non-null    float64
#   3   porcentaje  825 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  