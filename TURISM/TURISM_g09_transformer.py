from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	sort_values(how='descending', by=['ratio_balanza_pib'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 186 entries, 0 to 185
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     186 non-null    object 
#   1   geonombreFundar     186 non-null    object 
#   2   balanza             186 non-null    float64
#   3   gdp_current_prices  186 non-null    float64
#   4   ratio_balanza_pib   186 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |     balanza |   gdp_current_prices |   ratio_balanza_pib |
#  |---:|:------------------|:------------------|------------:|---------------------:|--------------------:|
#  |  0 | MAC               | Macao             | 2.38466e+11 |          3.32843e+11 |             71.6452 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by=['ratio_balanza_pib'])
#  RangeIndex: 186 entries, 0 to 185
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     186 non-null    object 
#   1   geonombreFundar     186 non-null    object 
#   2   balanza             186 non-null    float64
#   3   gdp_current_prices  186 non-null    float64
#   4   ratio_balanza_pib   186 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |     balanza |   gdp_current_prices |   ratio_balanza_pib |
#  |---:|:------------------|:------------------|------------:|---------------------:|--------------------:|
#  |  0 | MAC               | Macao             | 2.38466e+11 |          3.32843e+11 |             71.6452 |
#  
#  ------------------------------
#  