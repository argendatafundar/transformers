from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	ordenar_dos_columnas(col1='anio', order1=[1950, 1960, 1965, 1970, 1975, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024], col2='geonombreFundar', order2=['Europa', 'Asia y el Pacífico', 'Américas', 'Medio Oriente', 'África'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 235 entries, 0 to 234
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype  
#  ---  ------                                     --------------  -----  
#   0   anio                                       235 non-null    int64  
#   1   geocodigoFundar                            235 non-null    object 
#   2   geonombreFundar                            235 non-null    object 
#   3   arribos_turisticos_internacionales_millon  235 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turisticos_internacionales_millon |
#  |---:|-------:|:------------------|:------------------|--------------------------------------------:|
#  |  0 |   2024 | ZEUR              | Europa            |                                       758.6 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1950, 1960, 1965, 1970, 1975, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024], col2='geonombreFundar', order2=['Europa', 'Asia y el Pacífico', 'Américas', 'Medio Oriente', 'África'])
#  Index: 235 entries, 83 to 4
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype   
#  ---  ------                                     --------------  -----   
#   0   anio                                       235 non-null    category
#   1   geocodigoFundar                            235 non-null    object  
#   2   geonombreFundar                            235 non-null    category
#   3   arribos_turisticos_internacionales_millon  235 non-null    float64 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   arribos_turisticos_internacionales_millon |
#  |---:|-------:|:------------------|:------------------|--------------------------------------------:|
#  | 83 |   1950 | ZEUR              | Europa            |                                        16.8 |
#  
#  ------------------------------
#  