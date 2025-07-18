from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def rescale(df:DataFrame, group_cols:list[str], summarised_col:str, new_col:str) -> DataFrame:
    df[new_col] = df.groupby(group_cols)[summarised_col].transform(
    lambda x: 100*(x/x.sum()))
    return df

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict, default:Any = None)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, default if default is not None else x))
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	map_categoria(curr_col='comp_dem_ag', new_col='comp_dem_ag_neto', mapper={'Exportaciones': 'Exportaciones netas', 'Importaciones': 'Exportaciones netas', 'Formacion bruta de capital': 'Inversión', 'Variacion de existencias': 'Inversión'}, default=None),
	agg_sum(key_cols=['anio', 'comp_dem_ag_neto'], summarised_col='valor'),
	query(condition='anio in [1935, 1953, 1970, 1987, 2005, 2022]'),
	rescale(group_cols=['anio'], summarised_col='valor', new_col='valor_scaled')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         528 non-null    int64  
#   1   comp_dem_ag  528 non-null    object 
#   2   valor        528 non-null    float64
#  
#  |    |   anio | comp_dem_ag     |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='comp_dem_ag', new_col='comp_dem_ag_neto', mapper={'Exportaciones': 'Exportaciones netas', 'Importaciones': 'Exportaciones netas', 'Formacion bruta de capital': 'Inversión', 'Variacion de existencias': 'Inversión'}, default=None)
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              528 non-null    int64  
#   1   comp_dem_ag       528 non-null    object 
#   2   valor             528 non-null    float64
#   3   comp_dem_ag_neto  528 non-null    object 
#  
#  |    |   anio | comp_dem_ag     |   valor | comp_dem_ag_neto   |
#  |---:|-------:|:----------------|--------:|:-------------------|
#  |  0 |   1935 | Consumo hogares | 75.7468 | Consumo hogares    |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['anio', 'comp_dem_ag_neto'], summarised_col='valor')
#  RangeIndex: 352 entries, 0 to 351
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              352 non-null    int64  
#   1   comp_dem_ag_neto  352 non-null    object 
#   2   valor             352 non-null    float64
#  
#  |    |   anio | comp_dem_ag_neto   |   valor |
#  |---:|-------:|:-------------------|--------:|
#  |  0 |   1935 | Consumo gobierno   | 9.63011 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1935, 1953, 1970, 1987, 2005, 2022]')
#  Index: 24 entries, 0 to 351
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              24 non-null     int64  
#   1   comp_dem_ag_neto  24 non-null     object 
#   2   valor             24 non-null     float64
#   3   valor_scaled      24 non-null     float64
#  
#  |    |   anio | comp_dem_ag_neto   |   valor |   valor_scaled |
#  |---:|-------:|:-------------------|--------:|---------------:|
#  |  0 |   1935 | Consumo gobierno   | 9.63011 |        9.63011 |
#  
#  ------------------------------
#  
#  rescale(group_cols=['anio'], summarised_col='valor', new_col='valor_scaled')
#  Index: 24 entries, 0 to 351
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              24 non-null     int64  
#   1   comp_dem_ag_neto  24 non-null     object 
#   2   valor             24 non-null     float64
#   3   valor_scaled      24 non-null     float64
#  
#  |    |   anio | comp_dem_ag_neto   |   valor |   valor_scaled |
#  |---:|-------:|:-------------------|--------:|---------------:|
#  |  0 |   1935 | Consumo gobierno   | 9.63011 |        9.63011 |
#  
#  ------------------------------
#  