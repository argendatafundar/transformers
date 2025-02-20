from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	drop_na(cols=['valor']),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TSA','TEA', 'EAP'])")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             8962 non-null   object 
#   1   anio             8962 non-null   int64  
#   2   pib_pc           8962 non-null   float64
#   3   cambio_relativo  8962 non-null   float64
#  
#  |    | iso3   |   anio |   pib_pc |   cambio_relativo |
#  |---:|:-------|-------:|---------:|------------------:|
#  |  0 | AFE    |   2023 |  1418.36 |       -0.00992672 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  8962 non-null   object 
#   1   anio       8962 non-null   int64  
#   2   pib_pc     8962 non-null   float64
#   3   valor      8962 non-null   float64
#  
#  |    | geocodigo   |   anio |   pib_pc |       valor |
#  |---:|:------------|-------:|---------:|------------:|
#  |  0 | AFE         |   2023 |  1418.36 | -0.00992672 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  8962 non-null   object 
#   1   anio       8962 non-null   int64  
#   2   pib_pc     8962 non-null   float64
#   3   valor      8962 non-null   float64
#  
#  |    | geocodigo   |   anio |   pib_pc |     valor |
#  |---:|:------------|-------:|---------:|----------:|
#  |  0 | AFE         |   2023 |  1418.36 | -0.992672 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  8962 non-null   object 
#   1   anio       8962 non-null   int64  
#   2   pib_pc     8962 non-null   float64
#   3   valor      8962 non-null   float64
#  
#  |    | geocodigo   |   anio |   pib_pc |     valor |
#  |---:|:------------|-------:|---------:|----------:|
#  |  0 | AFE         |   2023 |  1418.36 | -0.992672 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  8962 non-null   object 
#   1   anio       8962 non-null   int64  
#   2   pib_pc     8962 non-null   float64
#   3   valor      8962 non-null   float64
#  
#  |    | geocodigo   |   anio |   pib_pc |   valor |
#  |---:|:------------|-------:|---------:|--------:|
#  |  0 | AFE         |   1975 |  1432.58 |       0 |
#  
#  ------------------------------
#  
#  query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TSA','TEA', 'EAP'])")
#  Index: 8521 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  8521 non-null   object 
#   1   anio       8521 non-null   int64  
#   2   pib_pc     8521 non-null   float64
#   3   valor      8521 non-null   float64
#  
#  |    | geocodigo   |   anio |   pib_pc |   valor |
#  |---:|:------------|-------:|---------:|--------:|
#  |  0 | AFE         |   1975 |  1432.58 |       0 |
#  
#  ------------------------------
#  