from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'sector': 'indicador', 'balanza': 'valor'}),
	ordenar_dos_columnas(col1='anio', order1=[2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022], col2='indicador', order2=['Investigación y desarrollo', 'SSIServicios profesionales', 'Ss. arquitectura, ingeniería y otros', 'Ss. audiovisuales', 'Propiedad intelectual'])
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
#   0   anio       102 non-null    category
#   1   indicador  68 non-null     category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador             |    valor |
#  |---:|-------:|:----------------------|---------:|
#  |  0 |   2006 | Propiedad intelectual | -814.312 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022], col2='indicador', order2=['Investigación y desarrollo', 'SSIServicios profesionales', 'Ss. arquitectura, ingeniería y otros', 'Ss. audiovisuales', 'Propiedad intelectual'])
#  Index: 102 entries, 34 to 67
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    category
#   1   indicador  68 non-null     category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  | 34 |   2006 | Investigación y desarrollo | 106.637 |
#  
#  ------------------------------
#  