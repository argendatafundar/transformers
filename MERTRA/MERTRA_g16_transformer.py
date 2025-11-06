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
	query(condition='nivel_ed_fundar != "Total"'),
	multiplicar_por_escalar(col='ocupado', k=100),
	drop_col(col=['geocodigoFundar', 'anio'], axis=1),
	long_to_wide(index=['nivel_ed_fundar'], columns='geonombreFundar', values='ocupado'),
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
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |  0.691307 |
#  
#  ------------------------------
#  
#  query(condition='nivel_ed_fundar != "Total"')
#  Index: 72 entries, 495 to 566
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  72 non-null     object 
#   1   geonombreFundar  72 non-null     object 
#   2   anio             72 non-null     int64  
#   3   nivel_ed_fundar  72 non-null     object 
#   4   ocupado          72 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='ocupado', k=100)
#  Index: 72 entries, 495 to 566
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  72 non-null     object 
#   1   geonombreFundar  72 non-null     object 
#   2   anio             72 non-null     int64  
#   3   nivel_ed_fundar  72 non-null     object 
#   4   ocupado          72 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:------------------|-------:|:----------------------------|----------:|
#  | 495 | AR-B              | Buenos Aires      |   2023 | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'anio'], axis=1)
#  Index: 72 entries, 495 to 566
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  72 non-null     object 
#   1   nivel_ed_fundar  72 non-null     object 
#   2   ocupado          72 non-null     float64
#  
#  |     | geonombreFundar   | nivel_ed_fundar             |   ocupado |
#  |----:|:------------------|:----------------------------|----------:|
#  | 495 | Buenos Aires      | Hasta secundario incompleto |   69.1307 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['nivel_ed_fundar'], columns='geonombreFundar', values='ocupado')
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 25 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   nivel_ed_fundar      3 non-null      object 
#   1   buenos_aires         3 non-null      float64
#   2   caba                 3 non-null      float64
#   3   catamarca            3 non-null      float64
#   4   chaco                3 non-null      float64
#   5   chubut               3 non-null      float64
#   6   corrientes           3 non-null      float64
#   7   cordoba              3 non-null      float64
#   8   entre_rios           3 non-null      float64
#   9   formosa              3 non-null      float64
#   10  jujuy                3 non-null      float64
#   11  la_pampa             3 non-null      float64
#   12  la_rioja             3 non-null      float64
#   13  mendoza              3 non-null      float64
#   14  misiones             3 non-null      float64
#   15  neuquen              3 non-null      float64
#   16  rio_negro            3 non-null      float64
#   17  salta                3 non-null      float64
#   18  san_juan             3 non-null      float64
#   19  san_luis             3 non-null      float64
#   20  santa_cruz           3 non-null      float64
#   21  santa_fe             3 non-null      float64
#   22  santiago_del_estero  3 non-null      float64
#   23  tierra_del_fuego     3 non-null      float64
#   24  tucuman              3 non-null      float64
#  
#  |    | nivel_ed_fundar             |   buenos_aires |    caba |   catamarca |   chaco |   chubut |   corrientes |   cordoba |   entre_rios |   formosa |   jujuy |   la_pampa |   la_rioja |   mendoza |   misiones |   neuquen |   rio_negro |   salta |   san_juan |   san_luis |   santa_cruz |   santa_fe |   santiago_del_estero |   tierra_del_fuego |   tucuman |
#  |---:|:----------------------------|---------------:|--------:|------------:|--------:|---------:|-------------:|----------:|-------------:|----------:|--------:|-----------:|-----------:|----------:|-----------:|----------:|------------:|--------:|-----------:|-----------:|-------------:|-----------:|----------------------:|-------------------:|----------:|
#  |  0 | Hasta secundario incompleto |        69.1307 | 74.4228 |     69.3035 | 55.1061 |  69.7116 |      61.9226 |   67.8936 |      70.8205 |   57.0821 | 72.6062 |    68.4995 |    66.3855 |   67.0162 |    64.9935 |   68.4998 |     64.1322 | 69.8702 |    67.3953 |    69.7485 |      62.9233 |    72.2288 |               66.6547 |            71.8944 |   68.8649 |
#  
#  ------------------------------
#  
#  normalize_colnames()
#  RangeIndex: 3 entries, 0 to 2
#  Data columns (total 25 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   nivel_ed_fundar      3 non-null      object 
#   1   buenos_aires         3 non-null      float64
#   2   caba                 3 non-null      float64
#   3   catamarca            3 non-null      float64
#   4   chaco                3 non-null      float64
#   5   chubut               3 non-null      float64
#   6   corrientes           3 non-null      float64
#   7   cordoba              3 non-null      float64
#   8   entre_rios           3 non-null      float64
#   9   formosa              3 non-null      float64
#   10  jujuy                3 non-null      float64
#   11  la_pampa             3 non-null      float64
#   12  la_rioja             3 non-null      float64
#   13  mendoza              3 non-null      float64
#   14  misiones             3 non-null      float64
#   15  neuquen              3 non-null      float64
#   16  rio_negro            3 non-null      float64
#   17  salta                3 non-null      float64
#   18  san_juan             3 non-null      float64
#   19  san_luis             3 non-null      float64
#   20  santa_cruz           3 non-null      float64
#   21  santa_fe             3 non-null      float64
#   22  santiago_del_estero  3 non-null      float64
#   23  tierra_del_fuego     3 non-null      float64
#   24  tucuman              3 non-null      float64
#  
#  |    | nivel_ed_fundar             |   buenos_aires |    caba |   catamarca |   chaco |   chubut |   corrientes |   cordoba |   entre_rios |   formosa |   jujuy |   la_pampa |   la_rioja |   mendoza |   misiones |   neuquen |   rio_negro |   salta |   san_juan |   san_luis |   santa_cruz |   santa_fe |   santiago_del_estero |   tierra_del_fuego |   tucuman |
#  |---:|:----------------------------|---------------:|--------:|------------:|--------:|---------:|-------------:|----------:|-------------:|----------:|--------:|-----------:|-----------:|----------:|-----------:|----------:|------------:|--------:|-----------:|-----------:|-------------:|-----------:|----------------------:|-------------------:|----------:|
#  |  0 | Hasta secundario incompleto |        69.1307 | 74.4228 |     69.3035 | 55.1061 |  69.7116 |      61.9226 |   67.8936 |      70.8205 |   57.0821 | 72.6062 |    68.4995 |    66.3855 |   67.0162 |    64.9935 |   68.4998 |     64.1322 | 69.8702 |    67.3953 |    69.7485 |      62.9233 |    72.2288 |               66.6547 |            71.8944 |   68.8649 |
#  
#  ------------------------------
#  