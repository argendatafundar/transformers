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
	ordenar_dos_columnas(col1='anio', order1=[1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], col2='region', order2=['Asia', 'Europa', 'Estados Unidos', 'América Latina', 'África', 'Otros'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 324 entries, 0 to 323
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               324 non-null    int64  
#   1   region             324 non-null    object 
#   2   industry_gdp       324 non-null    float64
#   3   prop_industry_gdp  324 non-null    float64
#  
#  |    |   anio | region         |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:---------------|---------------:|--------------------:|
#  |  0 |   1970 | América Latina |    2.42729e+11 |              8.5589 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], col2='region', order2=['Asia', 'Europa', 'Estados Unidos', 'América Latina', 'África', 'Otros'])
#  Index: 324 entries, 1 to 322
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype   
#  ---  ------             --------------  -----   
#   0   anio               324 non-null    category
#   1   region             324 non-null    category
#   2   industry_gdp       324 non-null    float64 
#   3   prop_industry_gdp  324 non-null    float64 
#  
#  |    |   anio | region   |   industry_gdp |   prop_industry_gdp |
#  |---:|-------:|:---------|---------------:|--------------------:|
#  |  1 |   1970 | Asia     |    4.28722e+11 |             15.1172 |
#  
#  ------------------------------
#  