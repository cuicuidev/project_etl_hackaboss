import plotly.express as px

from datetime import datetime
import pandas as pd
import numpy as np

# custom libraries
from util import _unpackLists, _concatCounts

def languages_fig(dataframe):
    #se crea una lista con todos los supports de audio y se mete en un dataframe
    audio_languages_df = _unpackLists(dataframe['audio_language_supports'])

    #se hace lo mismo con los subtitulos
    subtitles_languages_df = _unpackLists(dataframe['subtitles_language_supports'])


    #se crea un nuevo dataframe con la columna language, audio_count y subtitles_count.
    #este dataframe contiene las veces que aparece un lenguaje en audio_language_supports y subtitles_language_supports
    grouped_language_supports_df = _concatCounts(data_frames = [audio_languages_df, subtitles_languages_df],
                              columns = ['audio_language_supports', 'subtitles_language_supports'],
                              main_column = 'language',
                  axis = 1
                 )

    grouped_language_supports_df['total_count'] = grouped_language_supports_df['audio_language_supports_count'] + grouped_language_supports_df['subtitles_language_supports_count']

    languages_fig = px.bar(data_frame = grouped_language_supports_df.sort_values('total_count', ascending = True),
           y = 'language',
           x = ['audio_language_supports_count', 'subtitles_language_supports_count']
          ).update_layout(width = 900, height = 800)
    
    return languages_fig

def releases_fig(dataframe):
    #agrupamos por mes y año y sacamos los counts de cada mes de cada año
    grouped_df = dataframe.groupby(['month_release', 'year_release']).size().reset_index(name='count')

    #casteamos a int los años y meses para evitar errores en el cast a string
    grouped_df['year_release'] = grouped_df['year_release'].astype(int)

    grouped_df['month_release'] = grouped_df['month_release'].astype(int)

    #ordenamos los meses
    grouped_df = grouped_df.sort_values(['month_release', 'year_release'])

    #cambiamos los meses de número a string después de ordenarlos
    grouped_df["month_release"] = grouped_df["month_release"].apply(lambda x : datetime.strptime(f'{x}', "%m").strftime("%b"))

    #agrupamos por año y sacamos los counts de cada año. Esto es para el calculo del promedio
    grouped_months = dataframe.groupby('year_release').size().reset_index(name='count')

    #dividimos los counts entre 12 para el promedio
    grouped_months['count'] = grouped_months['count'].apply(lambda x : x/12)

    #modificamos la columna de month_release para que en lugar de aparecer un mes aparezca MEAN en el color del plotly express
    grouped_months['month_release'] = ['MEAN' for x in grouped_months.iloc]

    #concatenamos el dataframe del promedio con el de los meses agrupados verticalmente
    grouped_df = pd.concat([grouped_df, grouped_months], axis = 0)

    #creamos la gráfica con grouped_df
    releases_fig = px.line(data_frame = grouped_df,
            x = 'year_release',
            y = 'count',
            color = 'month_release',
            color_discrete_sequence=["deepskyblue", "royalblue", "hotpink", 'orchid', 'deeppink', 'palegreen', 'lawngreen', 'lime', 'navajowhite', "bisque", "peachpuff", 'dodgerblue', 'black'],
           )
    return releases_fig

def engines_fig(dataframe):
    #se crea una lista con todos los game_engines y se mete en un dataframe
    engines_df = _unpackLists(dataframe['game_engines'])

    #se cuenta cuantas veces aparece cada motor y se crea un nuevo dataframe con los datos
    engine_counts_df = pd.DataFrame(engines_df.value_counts()).rename(columns = {0 : 'count'}).reset_index().rename(columns = {0 : 'engines'})

    #se asigna una etiqueta top5 a cada motor para una visualización más interactiva
    engine_counts_df = engine_counts_df.sort_values('count', ascending = False)
    engine_counts_df['top'] = [f'Top {(idx // 5 + 1)*5}' for idx in engine_counts_df.index]

    #creamos el histograma con engine_counts_df
    engines_fig = px.histogram(data_frame = engine_counts_df.head(25).sort_values('count'),
                 y          = "game_engines",
                 x          = "count",
                 color = 'top'
                )
    return engines_fig.update_layout(width = 900, height = 800)

def engines_years_fig(dataframe):
    engines_years_df = dataframe[['game_engines', 'year_release']].dropna()

    engines_only = _unpackLists(dataframe['game_engines'])
    engine_counts_df = pd.DataFrame(engines_only.value_counts()).rename(columns = {0 : 'count'}).reset_index().rename(columns = {0 : 'engines'})
    top10 = list(engine_counts_df.sort_values('count', ascending = False).head(10)['game_engines'])

    engines_year_dict = {}

    for year in engines_years_df['year_release'].iloc:
        engines_year_dict[year] = []

    for engines, year in zip(engines_years_df['game_engines'].iloc, engines_years_df['year_release'].iloc):
        engines_year_dict[year].extend(engines)

    for year, engines in engines_year_dict.items():
        engines_dict = {}

        engines_ = []
        for engine in dataframe['game_engines'].iloc:
            if isinstance(engine, (list, np.ndarray)):
                engines_.extend(engine)

        unique_engines = list(set(engines_))

        for engine in unique_engines:
            engines_dict[engine] = 0

        for engine in engines:
            engines_dict[engine] = engines_dict[engine] + 1

        engines_year_dict[year] = engines_dict

    df_melt = pd.DataFrame(engines_year_dict).reset_index().melt(id_vars='index', var_name='Year', value_name='Value')
    df_melt.rename(columns={'index':'Engine'}, inplace=True)

    df_melt = df_melt[df_melt['Engine'].isin(top10)]

    engines_years_fig = px.line(data_frame = df_melt.sort_values(['Year', 'Value'], ascending = True),
                                x = 'Year',
                                y = 'Value',
                                color = 'Engine',
                               )
    return engines_years_fig

def genres_categories_fig(dataframe):
    genres_categories_df = dataframe[['genres', 'category']].dropna()

    genres_categories_dict = {}

    for category in genres_categories_df['category'].iloc:
        genres_categories_dict[category] = []

    for genres, category in zip(genres_categories_df['genres'].iloc, genres_categories_df['category'].iloc):
        genres_categories_dict[category].extend(genres)

    for category, genres in genres_categories_dict.items():
        genres_dict = {}

        genres_ = []
        for genre in dataframe['genres'].iloc:
            if isinstance(genre, (list, np.ndarray)):
                genres_.extend(genre)

        unique_genres = list(set(genres_))

        for genre in unique_genres:
            genres_dict[genre] = 0

        for genre in genres:
            genres_dict[genre] = genres_dict[genre] + 1

        genres_categories_dict[category] = genres_dict

    df_melt = pd.DataFrame(genres_categories_dict).reset_index().melt(id_vars='index', var_name='category', value_name='value')
    df_melt.rename(columns={'index':'genre'}, inplace=True)
    
    genres_categories_fig = px.treemap(data_frame = df_melt,
                                   values = 'value',
                                   path       = ['genre', 'category'],
                                   color      = 'genre',
                                   color_continuous_scale = 'green'
                                  )
    return genres_categories_fig