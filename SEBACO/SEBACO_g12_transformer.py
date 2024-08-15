from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(sector_sbc='categoria', salario_promedio='valor'),
	replace_value(col='categoria', curr_value='Ss. publicidad', new_value='Publicidad'),
	replace_value(col='categoria', curr_value='Ss. Arquitectura', new_value='Arquitectura'),
	replace_value(col='categoria', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables')
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
#  rename_columns(sector_sbc='categoria', salario_promedio='valor')
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       216 non-null    int64  
#   1   categoria  216 non-null    object 
#   2   valor      216 non-null    float64
#  
#  |    |   anio | categoria                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1996 | Investigación y desarrollo |  540329 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. publicidad', new_value='Publicidad')
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       216 non-null    int64  
#   1   categoria  216 non-null    object 
#   2   valor      216 non-null    float64
#  
#  |    |   anio | categoria                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1996 | Investigación y desarrollo |  540329 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. Arquitectura', new_value='Arquitectura')
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       216 non-null    int64  
#   1   categoria  216 non-null    object 
#   2   valor      216 non-null    float64
#  
#  |    |   anio | categoria                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1996 | Investigación y desarrollo |  540329 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables')
#  RangeIndex: 216 entries, 0 to 215
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       216 non-null    int64  
#   1   categoria  216 non-null    object 
#   2   valor      216 non-null    float64
#  
#  |    |   anio | categoria                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   1996 | Investigación y desarrollo |  540329 |
#  
#  ------------------------------
#  