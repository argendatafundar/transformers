from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='finalidad', replacements={'Gasto público social': 'Gasto social', 'Gasto público en servicios económicos': 'Servicios económicos', 'Servicios de la deuda pública': 'Deuda pública'}),
	ordenar_dos_columnas(col1='anio', order1=[1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], col2='finalidad', order2=['Gasto social', 'Funcionamiento del estado', 'Servicios económicos', 'Deuda pública'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       176 non-null    int64  
#   1   finalidad                  176 non-null    object 
#   2   gasto_publico_consolidado  176 non-null    float64
#  
#  |    |   anio | finalidad                 |   gasto_publico_consolidado |
#  |---:|-------:|:--------------------------|----------------------------:|
#  |  0 |   1980 | Funcionamiento del estado |                     5.36965 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='finalidad', replacements={'Gasto público social': 'Gasto social', 'Gasto público en servicios económicos': 'Servicios económicos', 'Servicios de la deuda pública': 'Deuda pública'})
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype   
#  ---  ------                     --------------  -----   
#   0   anio                       176 non-null    category
#   1   finalidad                  176 non-null    category
#   2   gasto_publico_consolidado  176 non-null    float64 
#  
#  |    |   anio | finalidad                 |   gasto_publico_consolidado |
#  |---:|-------:|:--------------------------|----------------------------:|
#  |  0 |   1980 | Funcionamiento del estado |                     5.36965 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], col2='finalidad', order2=['Gasto social', 'Funcionamiento del estado', 'Servicios económicos', 'Deuda pública'])
#  Index: 176 entries, 44 to 175
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype   
#  ---  ------                     --------------  -----   
#   0   anio                       176 non-null    category
#   1   finalidad                  176 non-null    category
#   2   gasto_publico_consolidado  176 non-null    float64 
#  
#  |    |   anio | finalidad    |   gasto_publico_consolidado |
#  |---:|-------:|:-------------|----------------------------:|
#  | 44 |   1980 | Gasto social |                     14.5055 |
#  
#  ------------------------------
#  