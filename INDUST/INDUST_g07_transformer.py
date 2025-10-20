from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="anio == anio.max() & geocodigoFundar == 'ARG'")
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
#  |      |   anio | geocodigoFundar   | geonombreFundar   | lall_code   | lall_desc_full                  |   exportaciones |      prop | source                 |
#  |-----:|-------:|:------------------|:------------------|:------------|:--------------------------------|----------------:|----------:|:-----------------------|
#  | 2366 |   2023 | ARG               | Argentina         | mat         | Manufacturas de alta tecnología |     1.47161e+09 | 0.0236206 | proyeccion_indice_baci |
#  
#  ------------------------------
#  