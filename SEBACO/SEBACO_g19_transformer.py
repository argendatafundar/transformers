from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'sector': 'indicador', 'balanza': 'valor'}),
	ordenar_categorica(col1='indicador', order1=['Servicios profesionales', 'SSI', 'Investigación y desarrollo', 'Ss. arquitectura, ingeniería y otros', 'Ss. audiovisuales', 'Propiedad intelectual'])
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
#  rename_cols(map={'sector': 'indicador', 'balanza': 'valor'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    int64   
#   1   indicador  102 non-null    category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador             |    valor |
#  |---:|-------:|:----------------------|---------:|
#  |  0 |   2006 | Propiedad intelectual | -814.312 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Servicios profesionales', 'SSI', 'Investigación y desarrollo', 'Ss. arquitectura, ingeniería y otros', 'Ss. audiovisuales', 'Propiedad intelectual'])
#  Index: 102 entries, 56 to 0
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    int64   
#   1   indicador  102 non-null    category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador               |   valor |
#  |---:|-------:|:------------------------|--------:|
#  | 56 |   2011 | Servicios profesionales | 1193.73 |
#  
#  ------------------------------
#  