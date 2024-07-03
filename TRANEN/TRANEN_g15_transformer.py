from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')

@transformer.convert
def recalculo_kaya(df: DataFrame, group: str, date_col: str):
    
    import pandas as pd

    # Filter data from the year 1965 onwards
    data = df.loc[df[date_col] >= 1965].copy()

    # Get all column names except 'anio' and 'iso3'
    variables = [col for col in data.columns if col not in [date_col, group]]

    # Get unique country codes
    codes = data[group].unique()

    # print(codes)

    # Initialize an empty list to store the modified dataframes
    data_list = []

    # Iterate through each country code
    for j in codes:

        # Filter data for the current country code
        data_code = data.loc[data[group] == j].copy()

        data_code.sort_values(by=date_col, inplace=True)

        # Iterate through each variable
        for i in variables:
            #print(i)
            # Filter out rows where the current variable is not NaN
            datos = data_code.dropna(subset=[i])

            if datos.shape[0] == 0:
                continue
            
            # Get the baseline value from the earliest year
            baseline = datos.loc[datos[date_col].idxmin(), i]
            
            # Calculate the base index
            data_code[i] = ((data_code[i] / baseline)-1) * 100


        # Append the modified dataframe to the list
        data_list.append(data_code)

    # Concatenate the modified dataframes
    data = pd.concat(data_list)
    return data


    #data

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	sort_values(how='ascending', by=['anio', 'iso3']),
	recalculo_kaya(group='iso3', date_col='anio'),
	query(condition='iso3 == "WLD"'),
	rename_cols(map={'iso3': 'geocodigo'}),
	wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='categoria'),
	replace_value(col='categoria', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2'),
	replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética (por unidad de PIB medido en dólares de 2011 PPA)'),
	replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares PPA 2011'),
	replace_value(col='categoria', curr_value='poblacion', new_value='Población'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono (CO2/kWh)'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011', new_value='Intensidad de carbono (CO2/$ PPA 2011)'),
	drop_col(col='geocodigo', axis=1),
	sort_values(how='ascending', by=['anio', 'categoria'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 201096 entries, 0 to 201095
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count   Dtype  
#  ---  ------                                --------------   -----  
#   0   anio                                  201096 non-null  int64  
#   1   iso3                                  201096 non-null  object 
#   2   emision_anual_co2_ton                 24157 non-null   float64
#   3   energia_por_unidad_pib_kwh            7789 non-null    float64
#   4   pib_per_cap_usd_ppa_2011              21314 non-null   float64
#   5   poblacion                             54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh           9328 non-null    float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  14242 non-null   float64
#  
#  |    |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |---:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  |  0 |   1750 | GBR    |             9.30594e+06 |                          nan |                       2702 | 9.28817e+06 |                           nan |                                    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 201096 entries, 0 to 201095
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count   Dtype  
#  ---  ------                                --------------   -----  
#   0   anio                                  201096 non-null  int64  
#   1   iso3                                  201096 non-null  object 
#   2   emision_anual_co2_ton                 24157 non-null   float64
#   3   energia_por_unidad_pib_kwh            7789 non-null    float64
#   4   pib_per_cap_usd_ppa_2011              21314 non-null   float64
#   5   poblacion                             54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh           9328 non-null    float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  14242 non-null   float64
#  
#  |    |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |---:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  |  0 |   1750 | GBR    |             9.30594e+06 |                          nan |                       2702 | 9.28817e+06 |                           nan |                                    nan |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'iso3'])
#  Index: 201096 entries, 71000 to 58526
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count   Dtype  
#  ---  ------                                --------------   -----  
#   0   anio                                  201096 non-null  int64  
#   1   iso3                                  201096 non-null  object 
#   2   emision_anual_co2_ton                 24157 non-null   float64
#   3   energia_por_unidad_pib_kwh            7789 non-null    float64
#   4   pib_per_cap_usd_ppa_2011              21314 non-null   float64
#   5   poblacion                             54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh           9328 non-null    float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  14242 non-null   float64
#  
#  |       |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |------:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  | 71000 | -10000 | ABW    |                     nan |                          nan |                        nan |         nan |                           nan |                                    nan |
#  
#  ------------------------------
#  
#  recalculo_kaya(group='iso3', date_col='anio')
#  Index: 14616 entries, 70439 to 58526
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  14616 non-null  int64  
#   1   iso3                                  14616 non-null  object 
#   2   emision_anual_co2_ton                 12091 non-null  float64
#   3   energia_por_unidad_pib_kwh            7789 non-null   float64
#   4   pib_per_cap_usd_ppa_2011              9509 non-null   float64
#   5   poblacion                             13783 non-null  float64
#   6   emision_anual_kgco2_por_kwh           9327 non-null   float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  9194 non-null   float64
#  
#  |       |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |------:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  | 70439 |   1965 | ABW    |                       0 |                          nan |                        nan |           0 |                           nan |                                    nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "WLD"')
#  Index: 58 entries, 11387 to 11444
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  58 non-null     int64  
#   1   iso3                                  58 non-null     object 
#   2   emision_anual_co2_ton                 58 non-null     float64
#   3   energia_por_unidad_pib_kwh            13 non-null     float64
#   4   pib_per_cap_usd_ppa_2011              13 non-null     float64
#   5   poblacion                             57 non-null     float64
#   6   emision_anual_kgco2_por_kwh           58 non-null     float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  13 non-null     float64
#  
#  |       |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |------:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  | 11387 |   1965 | WLD    |                       0 |                          nan |                        nan |           0 |                             0 |                                    nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  Index: 58 entries, 11387 to 11444
#  Data columns (total 8 columns):
#   #   Column                                Non-Null Count  Dtype  
#  ---  ------                                --------------  -----  
#   0   anio                                  58 non-null     int64  
#   1   geocodigo                             58 non-null     object 
#   2   emision_anual_co2_ton                 58 non-null     float64
#   3   energia_por_unidad_pib_kwh            13 non-null     float64
#   4   pib_per_cap_usd_ppa_2011              13 non-null     float64
#   5   poblacion                             57 non-null     float64
#   6   emision_anual_kgco2_por_kwh           58 non-null     float64
#   7   emision_anual_kgco2_por_usd_ppa_2011  13 non-null     float64
#  
#  |       |   anio | geocodigo   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |   emision_anual_kgco2_por_usd_ppa_2011 |
#  |------:|-------:|:------------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|---------------------------------------:|
#  | 11387 |   1965 | WLD         |                       0 |                          nan |                        nan |           0 |                             0 |                                    nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='categoria')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria             |   valor |
#  |---:|-------:|:------------|:----------------------|--------:|
#  |  0 |   1965 | WLD         | emision_anual_co2_ton |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética (por unidad de PIB medido en dólares de 2011 PPA)')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares PPA 2011')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='poblacion', new_value='Población')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono (CO2/kWh)')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011', new_value='Intensidad de carbono (CO2/$ PPA 2011)')
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   geocodigo  348 non-null    object 
#   2   categoria  348 non-null    object 
#   3   valor      212 non-null    float64
#  
#  |    |   anio | geocodigo   | categoria                |   valor |
#  |---:|-------:|:------------|:-------------------------|--------:|
#  |  0 |   1965 | WLD         | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigo', axis=1)
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   categoria  348 non-null    object 
#   2   valor      212 non-null    float64
#  
#  |    |   anio | categoria                |   valor |
#  |---:|-------:|:-------------------------|--------:|
#  |  0 |   1965 | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  Index: 348 entries, 0 to 231
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int64  
#   1   categoria  348 non-null    object 
#   2   valor      212 non-null    float64
#  
#  |    |   anio | categoria                |   valor |
#  |---:|-------:|:-------------------------|--------:|
#  |  0 |   1965 | Emisiones anuales de CO2 |       0 |
#  
#  ------------------------------
#  