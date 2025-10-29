from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="anio == anio.max() & geocodigoFundar == 'ARG'"),
	multiplicar_por_escalar(col='prop', k=100),
	sort_values(how='descending', by='prop'),
	ordenar_dos_columnas(col1='lall_desc_full', order1=['Productos primarios', 'Manufacturas basadas en recursos naturales', 'Manufacturas de baja tecnología', 'Manufacturas de media tecnología', 'Manufacturas de alta tecnología'], col2='lall_code', order2=['pp', 'mrrnn', 'mbt', 'mmt', 'mat'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62090 entries, 0 to 62089
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             62090 non-null  int64  
#   1   geocodigoFundar  62090 non-null  object 
#   2   geonombreFundar  62090 non-null  object 
#   3   lall_code        62090 non-null  object 
#   4   lall_desc_full   62090 non-null  object 
#   5   exportaciones    62090 non-null  float64
#   6   prop             62085 non-null  float64
#   7   source           62090 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full                  |   exportaciones |     prop | source         |
#  |---:|-------:|:------------------|:------------------|:------------|:--------------------------------|----------------:|---------:|:---------------|
#  |  0 |   1988 | ABW               | Aruba             | mat         | Manufacturas de alta tecnología |           28878 | 0.145207 | atlas_original |
#  
#  ------------------------------
#  
#  query(condition="anio == anio.max() & geocodigoFundar == 'ARG'")
#  Index: 5 entries, 2366 to 2614
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5 non-null      int64  
#   1   geocodigoFundar  5 non-null      object 
#   2   geonombreFundar  5 non-null      object 
#   3   lall_code        5 non-null      object 
#   4   lall_desc_full   5 non-null      object 
#   5   exportaciones    5 non-null      float64
#   6   prop             5 non-null      float64
#   7   source           5 non-null      object 
#  
#  |      |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full                  |   exportaciones |    prop | source                 |
#  |-----:|-------:|:------------------|:------------------|:------------|:--------------------------------|----------------:|--------:|:-----------------------|
#  | 2366 |   2023 | ARG               | Argentina         | mat         | Manufacturas de alta tecnología |     1.47161e+09 | 2.36206 | proyeccion_indice_baci |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  Index: 5 entries, 2366 to 2614
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5 non-null      int64  
#   1   geocodigoFundar  5 non-null      object 
#   2   geonombreFundar  5 non-null      object 
#   3   lall_code        5 non-null      object 
#   4   lall_desc_full   5 non-null      object 
#   5   exportaciones    5 non-null      float64
#   6   prop             5 non-null      float64
#   7   source           5 non-null      object 
#  
#  |      |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full                  |   exportaciones |    prop | source                 |
#  |-----:|-------:|:------------------|:------------------|:------------|:--------------------------------|----------------:|--------:|:-----------------------|
#  | 2366 |   2023 | ARG               | Argentina         | mat         | Manufacturas de alta tecnología |     1.47161e+09 | 2.36206 | proyeccion_indice_baci |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='prop')
#  RangeIndex: 5 entries, 0 to 4
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   anio             5 non-null      int64   
#   1   geocodigoFundar  5 non-null      object  
#   2   geonombreFundar  5 non-null      object  
#   3   lall_code        5 non-null      category
#   4   lall_desc_full   5 non-null      category
#   5   exportaciones    5 non-null      float64 
#   6   prop             5 non-null      float64 
#   7   source           5 non-null      object  
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full      |   exportaciones |    prop | source                 |
#  |---:|-------:|:------------------|:------------------|:------------|:--------------------|----------------:|--------:|:-----------------------|
#  |  0 |   2023 | ARG               | Argentina         | pp          | Productos primarios |     3.49511e+10 | 56.0994 | proyeccion_indice_baci |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='lall_desc_full', order1=['Productos primarios', 'Manufacturas basadas en recursos naturales', 'Manufacturas de baja tecnología', 'Manufacturas de media tecnología', 'Manufacturas de alta tecnología'], col2='lall_code', order2=['pp', 'mrrnn', 'mbt', 'mmt', 'mat'])
#  Index: 5 entries, 0 to 3
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   anio             5 non-null      int64   
#   1   geocodigoFundar  5 non-null      object  
#   2   geonombreFundar  5 non-null      object  
#   3   lall_code        5 non-null      category
#   4   lall_desc_full   5 non-null      category
#   5   exportaciones    5 non-null      float64 
#   6   prop             5 non-null      float64 
#   7   source           5 non-null      object  
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full      |   exportaciones |    prop | source                 |
#  |---:|-------:|:------------------|:------------------|:------------|:--------------------|----------------:|--------:|:-----------------------|
#  |  0 |   2023 | ARG               | Argentina         | pp          | Productos primarios |     3.49511e+10 | 56.0994 | proyeccion_indice_baci |
#  
#  ------------------------------
#  