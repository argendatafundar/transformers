from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'geocodigoFundar': 'geocodigo', 'geonombreFundar': 'geonombre', 'va_agro_sobre_pbi': 'valor'}),
	drop_na(subset=['valor']),
	sort_values(how='ascending', by=['anio', 'geonombre']),
	query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])"),
	query(condition='anio <= 2022')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    10772 non-null  object 
#   1   geonombreFundar    10772 non-null  object 
#   2   anio               10772 non-null  int64  
#   3   va_agro_sobre_pbi  10772 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   va_agro_sobre_pbi |
#  |---:|:------------------|:------------------|-------:|--------------------:|
#  |  0 | ABW               | Aruba             |   1995 |            0.505922 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo', 'geonombreFundar': 'geonombre', 'va_agro_sobre_pbi': 'valor'})
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   geonombre  10772 non-null  object 
#   2   anio       10772 non-null  int64  
#   3   valor      10772 non-null  float64
#  
#  |    | geocodigo   | geonombre   |   anio |    valor |
#  |---:|:------------|:------------|-------:|---------:|
#  |  0 | ABW         | Aruba       |   1995 | 0.505922 |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   geonombre  10772 non-null  object 
#   2   anio       10772 non-null  int64  
#   3   valor      10772 non-null  float64
#  
#  |    | geocodigo   | geonombre   |   anio |    valor |
#  |---:|:------------|:------------|-------:|---------:|
#  |  0 | ABW         | Aruba       |   1995 | 0.505922 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geonombre'])
#  RangeIndex: 10772 entries, 0 to 10771
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10772 non-null  object 
#   1   geonombre  10772 non-null  object 
#   2   anio       10772 non-null  int64  
#   3   valor      10772 non-null  float64
#  
#  |    | geocodigo   | geonombre   |   anio |   valor |
#  |---:|:------------|:------------|-------:|--------:|
#  |  0 | IBT         | AIF         |   1960 | 27.2059 |
#  
#  ------------------------------
#  
#  query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP', 'XT', 'XN', 'XM', 'XD'])")
#  Index: 10290 entries, 0 to 10769
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10290 non-null  object 
#   1   geonombre  10290 non-null  object 
#   2   anio       10290 non-null  int64  
#   3   valor      10290 non-null  float64
#  
#  |    | geocodigo   | geonombre   |   anio |   valor |
#  |---:|:------------|:------------|-------:|--------:|
#  |  0 | IBT         | AIF         |   1960 | 27.2059 |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2022')
#  Index: 10106 entries, 0 to 10574
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  10106 non-null  object 
#   1   geonombre  10106 non-null  object 
#   2   anio       10106 non-null  int64  
#   3   valor      10106 non-null  float64
#  
#  |    | geocodigo   | geonombre   |   anio |   valor |
#  |---:|:------------|:------------|-------:|--------:|
#  |  0 | IBT         | AIF         |   1960 | 27.2059 |
#  
#  ------------------------------
#  