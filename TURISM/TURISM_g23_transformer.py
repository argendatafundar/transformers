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
	ordenar_dos_columnas(col1='anio', order1=[1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024], col2='region', order2=['Europa', 'Asia', 'Américas', 'Oceanía', 'África'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 245 entries, 0 to 244
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             245 non-null    int64  
#   1   region           245 non-null    object 
#   2   expo_turisticas  245 non-null    float64
#  
#  |    |   anio | region   |   expo_turisticas |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1976 | Américas |           9.11311 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024], col2='region', order2=['Europa', 'Asia', 'Américas', 'Oceanía', 'África'])
#  Index: 245 entries, 2 to 244
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   anio             245 non-null    category
#   1   region           245 non-null    category
#   2   expo_turisticas  245 non-null    float64 
#  
#  |    |   anio | region   |   expo_turisticas |
#  |---:|-------:|:---------|------------------:|
#  |  2 |   1976 | Europa   |           20.3868 |
#  
#  ------------------------------
#  