from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_cols(cols=['unit']),
	to_pandas(dummy=True),
	rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=0.001),
	sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_cols(cols=['unit'])
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   sector           1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor_en_ggco2e  1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | sector   | geocodigoFundar   | entity               |   valor_en_ggco2e | geonombreFundar   |
#  |---:|-------:|:---------|:------------------|:---------------------|------------------:|:------------------|
#  |  0 |   1750 | AGSyOUT  | WLD               | KYOTOGHG (AR6GWP100) |            558000 | Mundo             |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'})
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  0 |   1750 | AGSyOUT     | WLD               | KYOTOGHG (AR6GWP100) |     558 | Mundo             |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=0.001)
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  0 |   1750 | AGSyOUT     | WLD               | KYOTOGHG (AR6GWP100) |     558 | Mundo             |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
#  Index: 1375 entries, 1 to 1372
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  1 |   1750 | Energía     | WLD               | KYOTOGHG (AR6GWP100) |     152 | Mundo             |
#  
#  ------------------------------
#  