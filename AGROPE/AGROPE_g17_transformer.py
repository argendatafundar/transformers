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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'tipo_carne': 'indicador'}),
	ordenar_dos_columnas(col1='indicador', order1=['Carne bovina', 'Carne aviar', 'Carne de cerdo'], col2='anio', order2=[1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   tipo_carne  183 non-null    object 
#   1   anio        183 non-null    int64  
#   2   valor       183 non-null    float64
#  
#  |    | tipo_carne   |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_carne': 'indicador'})
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  183 non-null    category
#   1   anio       183 non-null    category
#   2   valor      183 non-null    float64 
#  
#  |    | indicador    |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='indicador', order1=['Carne bovina', 'Carne aviar', 'Carne de cerdo'], col2='anio', order2=[1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021])
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  183 non-null    category
#   1   anio       183 non-null    category
#   2   valor      183 non-null    float64 
#  
#  |    | indicador    |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  