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
rename_cols(map={'sector_sbc': 'indicador'}),
	rename_cols(map={'salario_promedio': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              216 non-null    int64  
#   1   sector_sbc        216 non-null    object 
#   2   salario_promedio  216 non-null    float64
#  
#  |    |   anio | sector_sbc                 |   salario_promedio |
#  |---:|-------:|:---------------------------|-------------------:|
#  |  0 |   1996 | Investigación y desarrollo |             540329 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector_sbc': 'indicador'})
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              216 non-null    int64  
#   1   indicador         216 non-null    object 
#   2   salario_promedio  216 non-null    float64
#  
#  |    |   anio | indicador                  |   salario_promedio |
#  |---:|-------:|:---------------------------|-------------------:|
#  |  0 |   1996 | Investigación y desarrollo |             540329 |
#  
#  ------------------------------
#  
#  rename_cols(map={'salario_promedio': 'valor'})
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       216 non-null    int64  
#   1   indicador  216 non-null    object 
#   2   valor      216 non-null    float64
#  
#  |    |   anio | indicador                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1996 | Investigación y desarrollo |  540329 |
#  
#  ------------------------------
#  