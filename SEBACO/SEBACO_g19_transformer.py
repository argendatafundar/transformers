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
rename_columns(sector='indicador', balanza='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     102 non-null    int64  
#   1   sector   102 non-null    object 
#   2   balanza  102 non-null    float64
#  
#  |    |   anio | sector                |   balanza |
#  |---:|-------:|:----------------------|----------:|
#  |  0 |   2006 | Propiedad intelectual |  -814.312 |
#  
#  ------------------------------
#  
#  rename_columns(sector='indicador', balanza='valor')
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador             |    valor |
#  |---:|-------:|:----------------------|---------:|
#  |  0 |   2006 | Propiedad intelectual | -814.312 |
#  
#  ------------------------------
#  