from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def normalize_colnames(df: DataFrame):
    import unicodedata
    def remove_accents(text: str) -> str:
        # Normaliza y elimina caracteres con acentos
        nfkd = unicodedata.normalize('NFKD', text)
        return ''.join([c for c in nfkd if not unicodedata.combining(c)])

    df.columns = [
        remove_accents(str(x)).lower().replace(" ", "_")
        for x in df.columns
    ]
    return df

@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	multiplicar_por_escalar(col='ocupado', k=100),
	drop_col(col=['geocodigoFundar', 'anio'], axis=1),
	long_to_wide(index=['geonombreFundar'], columns='nivel_ed_fundar', values='ocupado'),
	normalize_colnames()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  756 non-null    object 
#   1   geonombreFundar  756 non-null    object 
#   2   anio             756 non-null    int64  
#   3   nivel_ed_fundar  756 non-null    object 
#   4   ocupado          756 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |---:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 | Hasta secundario incompleto |  0.647369 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 96 entries, 495 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  96 non-null     object 
#   1   geonombreFundar  96 non-null     object 
#   2   anio             96 non-null     int64  
#   3   nivel_ed_fundar  96 non-null     object 
#   4   ocupado          96 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='ocupado', k=100)
#  Index: 96 entries, 495 to 755
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  96 non-null     object 
#   1   geonombreFundar  96 non-null     object 
#   2   anio             96 non-null     int64  
#   3   nivel_ed_fundar  96 non-null     object 
#   4   ocupado          96 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'anio'], axis=1)
#  Index: 96 entries, 495 to 755
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  96 non-null     object 
#   1   nivel_ed_fundar  96 non-null     object 
#   2   ocupado          96 non-null     float64
#  
#  |     | geonombreFundar   | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:----------------------------|----------:|
#  | 495 | Buenos Aires      | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['geonombreFundar'], columns='nivel_ed_fundar', values='ocupado')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geonombrefundar                 24 non-null     object 
#   1   hasta_secundario_incompleto     24 non-null     float64
#   2   secundario_completo             24 non-null     float64
#   3   superior_incompleto_o_completo  24 non-null     float64
#   4   total                           24 non-null     float64
#  
#  |    | geonombrefundar   |   hasta_secundario_incompleto |   secundario_completo |   superior_incompleto_o_completo |   total |
#  |---:|:------------------|------------------------------:|----------------------:|---------------------------------:|--------:|
#  |  0 | Buenos Aires      |                       69.1307 |               76.2481 |                          85.2302 | 76.6514 |
#  
#  ------------------------------
#  
#  normalize_colnames()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geonombrefundar                 24 non-null     object 
#   1   hasta_secundario_incompleto     24 non-null     float64
#   2   secundario_completo             24 non-null     float64
#   3   superior_incompleto_o_completo  24 non-null     float64
#   4   total                           24 non-null     float64
#  
#  |    | geonombrefundar   |   hasta_secundario_incompleto |   secundario_completo |   superior_incompleto_o_completo |   total |
#  |---:|:------------------|------------------------------:|----------------------:|---------------------------------:|--------:|
#  |  0 | Buenos Aires      |                       69.1307 |               76.2481 |                          85.2302 | 76.6514 |
#  
#  ------------------------------
#  