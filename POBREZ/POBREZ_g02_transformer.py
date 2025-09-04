from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def quedarse_con_n_anios(df: DataFrame, year_col:str, n_years:int):
    years = df[year_col].sort_values().unique().tolist()
    L = len(years) - 1  # último índice
    idx = [round(i * L / (n_years - 1)) for i in range(n_years)]
    filter_years = [years[i] for i in idx]
    return  df[df[year_col].isin(filter_years)]

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	quedarse_con_n_anios(year_col='year', n_years=2),
	replace_multiple_values(col='province', replacements={'san_luis': 'San Luis', 'neuquen': 'Neuquén', 'santa_fe': 'Santa Fe', 'san_juan': 'San Juan', 'santiago_del_estero': 'Santiago del Estero', 'PBA': 'Interior de PBA', 'caba': 'CABA', 'la_pampa': 'La Pampa', 'tierra_del_fuego': 'Tierra del Fuego', 'salta': 'Salta', 'chubut': 'Chubut', 'cordoba': 'Córdoba', 'tucuman': 'Tucumán', 'jujuy': 'Jujuy', 'chaco': 'Chaco', 'la_rioja': 'La Rioja', 'catamarca': 'Catamarca', 'rio_negro': 'Río Negro', 'mendoza': 'Mendoza', 'formosa': 'Formosa', 'partidos_GBA': 'Partidos del GBA', 'santa_cruz': 'Santa Cruz', 'entre rios': 'Entre Ríos', 'corrientes': 'Corrientes', 'misiones': 'Misiones', 'argentina': 'Total Nacional'}),
	ordenar_dos_columnas(col1='province', order1=['Santiago del Estero', 'Formosa', 'Jujuy', 'Chaco', 'Catamarca', 'Misiones', 'Corrientes', 'Salta', 'Tucumán', 'Neuquén', 'Río Negro', 'Chubut', 'La Rioja', 'Entre Ríos', 'San Luis', 'San Juan', 'Santa Cruz', 'La Pampa', 'Total Nacional', 'Santa Fe', 'Córdoba', 'Partidos del GBA', 'Mendoza', 'Tierra del Fuego', 'Interior de PBA', 'CABA'], col2='year', order2=[1980, 2022])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 130 entries, 0 to 129
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   province  130 non-null    object 
#   1   year      130 non-null    int64  
#   2   nbi_rate  130 non-null    float64
#  
#  |    | province   |   year |   nbi_rate |
#  |---:|:-----------|-------:|-----------:|
#  |  0 | argentina  |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  quedarse_con_n_anios(year_col='year', n_years=2)
#  Index: 52 entries, 0 to 129
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   province  52 non-null     category
#   1   year      52 non-null     category
#   2   nbi_rate  52 non-null     float64 
#  
#  |    | province       |   year |   nbi_rate |
#  |---:|:---------------|-------:|-----------:|
#  |  0 | Total Nacional |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='province', replacements={'san_luis': 'San Luis', 'neuquen': 'Neuquén', 'santa_fe': 'Santa Fe', 'san_juan': 'San Juan', 'santiago_del_estero': 'Santiago del Estero', 'PBA': 'Interior de PBA', 'caba': 'CABA', 'la_pampa': 'La Pampa', 'tierra_del_fuego': 'Tierra del Fuego', 'salta': 'Salta', 'chubut': 'Chubut', 'cordoba': 'Córdoba', 'tucuman': 'Tucumán', 'jujuy': 'Jujuy', 'chaco': 'Chaco', 'la_rioja': 'La Rioja', 'catamarca': 'Catamarca', 'rio_negro': 'Río Negro', 'mendoza': 'Mendoza', 'formosa': 'Formosa', 'partidos_GBA': 'Partidos del GBA', 'santa_cruz': 'Santa Cruz', 'entre rios': 'Entre Ríos', 'corrientes': 'Corrientes', 'misiones': 'Misiones', 'argentina': 'Total Nacional'})
#  Index: 52 entries, 0 to 129
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   province  52 non-null     category
#   1   year      52 non-null     category
#   2   nbi_rate  52 non-null     float64 
#  
#  |    | province       |   year |   nbi_rate |
#  |---:|:---------------|-------:|-----------:|
#  |  0 | Total Nacional |   1980 |       22.3 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='province', order1=['Santiago del Estero', 'Formosa', 'Jujuy', 'Chaco', 'Catamarca', 'Misiones', 'Corrientes', 'Salta', 'Tucumán', 'Neuquén', 'Río Negro', 'Chubut', 'La Rioja', 'Entre Ríos', 'San Luis', 'San Juan', 'Santa Cruz', 'La Pampa', 'Total Nacional', 'Santa Fe', 'Córdoba', 'Partidos del GBA', 'Mendoza', 'Tierra del Fuego', 'Interior de PBA', 'CABA'], col2='year', order2=[1980, 2022])
#  Index: 52 entries, 115 to 9
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   province  52 non-null     category
#   1   year      52 non-null     category
#   2   nbi_rate  52 non-null     float64 
#  
#  |     | province            |   year |   nbi_rate |
#  |----:|:--------------------|-------:|-----------:|
#  | 115 | Santiago del Estero |   1980 |       45.8 |
#  
#  ------------------------------
#  