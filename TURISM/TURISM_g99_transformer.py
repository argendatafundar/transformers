from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def custom_logic(df: DataFrame) -> DataFrame:
    # separar los dos subconjuntos
    df_top10 = df[df['top_10'] == 'Top 10'].copy()
    df_resto = df[df['top_10'] == 'Resto destinos'].copy()

    # si hay filas del resto, agregarlas
    if not df_resto.empty:
        # reemplazar localidades
        df_resto.loc[:, 'localidad'] = 'Resto localidades'

        # agregar
        df_resto_agg = (
            df_resto
            .groupby(['residencia', 'localidad', 'top_10'], as_index=False)['share']
            .sum()
        )
    else:
        df_resto_agg = pd.DataFrame(columns=df.columns)

    # devolver todo junto
    df = pd.concat([df_top10.sort_values(by='share', ascending=False), df_resto_agg], ignore_index=True)

    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def columna_acumulada(df:DataFrame, cum_col:str)-> DataFrame: 
    df.loc[:, f'cumsum_{cum_col}'] = df.loc[:, cum_col].cumsum()

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='localidad', curr_value=None, new_value=None, mapping={'Caba': 'CABA', 'Mar Del Plata': 'Mar del Plata', 'Cordoba': 'Córdoba', 'Puerto Iguazu': 'Puerto Iguazú', 'San Martin De Los Andes': 'S. M. de los Andes'}),
	custom_logic(),
	query(condition="residencia == 'residentes'"),
	columna_acumulada(cum_col='share')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 104 entries, 0 to 103
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   residencia  104 non-null    object 
#   1   localidad   104 non-null    object 
#   2   share       104 non-null    float64
#   3   top_10      104 non-null    object 
#  
#  |    | residencia    | localidad    |    share | top_10         |
#  |---:|:--------------|:-------------|---------:|:---------------|
#  |  0 | no_residentes | Bahia Blanca | 0.132146 | Resto destinos |
#  
#  ------------------------------
#  
#  replace_value(col='localidad', curr_value=None, new_value=None, mapping={'Caba': 'CABA', 'Mar Del Plata': 'Mar del Plata', 'Cordoba': 'Córdoba', 'Puerto Iguazu': 'Puerto Iguazú', 'San Martin De Los Andes': 'S. M. de los Andes'})
#  RangeIndex: 104 entries, 0 to 103
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   residencia  104 non-null    object 
#   1   localidad   104 non-null    object 
#   2   share       104 non-null    float64
#   3   top_10      104 non-null    object 
#  
#  |    | residencia    | localidad    |    share | top_10         |
#  |---:|:--------------|:-------------|---------:|:---------------|
#  |  0 | no_residentes | Bahia Blanca | 0.132146 | Resto destinos |
#  
#  ------------------------------
#  
#  custom_logic()
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   residencia  22 non-null     object 
#   1   localidad   22 non-null     object 
#   2   share       22 non-null     float64
#   3   top_10      22 non-null     object 
#  
#  |    | residencia    | localidad   |   share | top_10   |
#  |---:|:--------------|:------------|--------:|:---------|
#  |  0 | no_residentes | CABA        | 59.5021 | Top 10   |
#  
#  ------------------------------
#  
#  query(condition="residencia == 'residentes'")
#  Index: 11 entries, 1 to 21
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   residencia    11 non-null     object 
#   1   localidad     11 non-null     object 
#   2   share         11 non-null     float64
#   3   top_10        11 non-null     object 
#   4   cumsum_share  11 non-null     float64
#  
#  |    | residencia   | localidad   |   share | top_10   |   cumsum_share |
#  |---:|:-------------|:------------|--------:|:---------|---------------:|
#  |  1 | residentes   | CABA        | 18.7903 | Top 10   |        18.7903 |
#  
#  ------------------------------
#  
#  columna_acumulada(cum_col='share')
#  Index: 11 entries, 1 to 21
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   residencia    11 non-null     object 
#   1   localidad     11 non-null     object 
#   2   share         11 non-null     float64
#   3   top_10        11 non-null     object 
#   4   cumsum_share  11 non-null     float64
#  
#  |    | residencia   | localidad   |   share | top_10   |   cumsum_share |
#  |---:|:-------------|:------------|--------:|:---------|---------------:|
#  |  1 | residentes   | CABA        | 18.7903 | Top 10   |        18.7903 |
#  
#  ------------------------------
#  