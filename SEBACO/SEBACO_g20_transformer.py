from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'descripcion': 'indicador', 'prop_sobre_sbc_expo': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	ordenar_dos_columnas(col1='anio', order1=[2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022], col2='indicador', order2=['Servicios profesionales', 'Ss. arquitectura, ingeniería y otros', 'SSI', 'Ss. audiovisuales', 'Investigación y desarrollo', 'Propiedad intelectual'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 102 non-null    int64  
#   1   descripcion          102 non-null    object 
#   2   prop_sobre_sbc_expo  102 non-null    float64
#  
#  |    |   anio | descripcion           |   prop_sobre_sbc_expo |
#  |---:|-------:|:----------------------|----------------------:|
#  |  0 |   2006 | Propiedad intelectual |             0.0319314 |
#  
#  ------------------------------
#  
#  rename_cols(map={'descripcion': 'indicador', 'prop_sobre_sbc_expo': 'valor'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    category
#   1   indicador  102 non-null    category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador             |   valor |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   2006 | Propiedad intelectual | 3.19314 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    category
#   1   indicador  102 non-null    category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador             |   valor |
#  |---:|-------:|:----------------------|--------:|
#  |  0 |   2006 | Propiedad intelectual | 3.19314 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022], col2='indicador', order2=['Servicios profesionales', 'Ss. arquitectura, ingeniería y otros', 'SSI', 'Ss. audiovisuales', 'Investigación y desarrollo', 'Propiedad intelectual'])
#  Index: 102 entries, 51 to 16
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       102 non-null    category
#   1   indicador  102 non-null    category
#   2   valor      102 non-null    float64 
#  
#  |    |   anio | indicador               |   valor |
#  |---:|-------:|:------------------------|--------:|
#  | 51 |   2006 | Servicios profesionales | 38.9265 |
#  
#  ------------------------------
#  