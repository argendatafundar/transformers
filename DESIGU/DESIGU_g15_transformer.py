from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def agregar_columna(df:DataFrame, valores:list, col_name :str): 
    df[col_name] = valores
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def apendear_dataframe(df:DataFrame, df_nuevo:dict):
    import pandas as pd
    df_nuevo = pd.DataFrame(df_nuevo)
    df = pd.concat([df, df_nuevo], axis = 0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
agregar_columna(valores=['Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación'], col_name='categoria'),
	rename_cols(map={'part_salarial_vab': 'valor'}),
	apendear_dataframe(df_nuevo={'anio': [1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], 'categoria': ['Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio'], 'valor': [40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40]})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               89 non-null     int64  
#   1   part_salarial_vab  89 non-null     float64
#   2   categoria          89 non-null     object 
#  
#  |    |   anio |   part_salarial_vab | categoria     |
#  |---:|-------:|--------------------:|:--------------|
#  |  0 |   1935 |              35.294 | Participación |
#  
#  ------------------------------
#  
#  agregar_columna(valores=['Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación', 'Participación'], col_name='categoria')
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               89 non-null     int64  
#   1   part_salarial_vab  89 non-null     float64
#   2   categoria          89 non-null     object 
#  
#  |    |   anio |   part_salarial_vab | categoria     |
#  |---:|-------:|--------------------:|:--------------|
#  |  0 |   1935 |              35.294 | Participación |
#  
#  ------------------------------
#  
#  rename_cols(map={'part_salarial_vab': 'valor'})
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       89 non-null     int64  
#   1   valor      89 non-null     float64
#   2   categoria  89 non-null     object 
#  
#  |    |   anio |   valor | categoria     |
#  |---:|-------:|--------:|:--------------|
#  |  0 |   1935 |  35.294 | Participación |
#  
#  ------------------------------
#  
#  apendear_dataframe(df_nuevo={'anio': [1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], 'categoria': ['Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio', 'Promedio'], 'valor': [40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40]})
#  Index: 178 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       178 non-null    int64  
#   1   valor      178 non-null    float64
#   2   categoria  178 non-null    object 
#  
#  |    |   anio |   valor | categoria     |
#  |---:|-------:|--------:|:--------------|
#  |  0 |   1935 |  35.294 | Participación |
#  
#  ------------------------------
#  