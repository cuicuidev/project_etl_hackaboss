# Proyecto ETL utilizando la API de IGDB de Twitch
### Instrucciones
##### Desde la consola, clonan el repositorio.
```sh
git clone https://github.com/cuicuidev/project_etl_hackaboss.git

```
##### Una vez clonado, navegan a la carpeta.
```sh
cd project_etl_hackaboss
```
##### Desde ahí, crean un entorno nuevo de python.
```sh
python3 -m venv venv
```
##### Una vez creado el entorno, lo activan.
- Para los usuarios de Windows:
```sh
.\venv\Scripts\activate
```
- Para los usuarios de Linux o de MacOS:
```sh
source venv/bin/activate
```
##### Acto seguido, instalan todas las dependencias.
```sh
pip install -r dependencies.txt
```
##### Por último, crean un archivo api.json y rellenan los campos con sus claves API siguiendo este formato.
```json
{
    "IGDB" : {
        "clientID" : "",
        "clientSecret" : ""
    },

    "Airtable" : {
        "key" : "",
        "app" : "",
        "tbls" : ["","","","",""]
    }
}
```
donde deberán rellenar los campos con sus claves personales de las APIs.

### Extracción de datos
Se ha utilizado la API de IGDB de Twitch para conseguir datos acerca de los videojuegos.
De entre todos los endpoints que ofrece la API, se han descargado los datos de 'games', 'game_engines', 'language_supports', 'languages' y 'genres'.

### Transformación de datos
Los datos de IGDB están estructurados de manera relacional y cada enpoint proporciona acceso a una tabla de la base de datos. La tabla objetivo de este proyecto fue la de 'games'.
Primero se han borrado gran parte de las columnas de la tabla principal al ser irrelevantes. Las columnas con las que se ha trabajado son las siguientes:
- 'id'
- 'name'
- 'language_supports'
- 'game_engines'
- 'first_release_date'
- 'genres'
- 'category'

Acto seguido, se han convertido los id's de las columnas 'language_supports', 'game_engines', 'genres' y 'category' por los nombres de las tablas correspondientes.
Para la columna de 'language_supports', se han separado los datos por soportes de audio y de subtítulos en dos nuevas columnas.
La columna de 'first_release_date' se transformó de timestamp a objeto datetime y se separó en dos columnas, una para el año y otra para el mes.
Por último, se eliminaron las columnas con datos en crudo. La tabla transformada quedó así:
- 'id',
- 'name',
- 'game_engines',
- 'genres',
- 'category',
- 'audio_language_supports',
- 'subtitles_language_supports',
- 'month_release',
- 'year_release'

### Carga de datos
Los datos transformados se cargaron a tablas de airtable utilizando la API. Se tuvieron que crear cinco tablas separadas por el gran tamaño del dataset obtenido de la API de IGDB, ya que cada tabla soporta un máximo de 50k filas. Utilizando la API también se ha automatizado la descarga de datos.

### Visualizaciones
Con los datos transformados se han obtenido las siguientes visualizaciones que pueden arrojar algo de luz sobre la situación actual de la industria de los videojuegos, así como su historia.


##### Fechas de salida por mes del año a lo largo de los años
![Fechas de salida](vis/releases_fig.png)
=
##### Top 10 Frameworks de desarrollo de videojuegos (general)
![Motores gŕaficos](vis/engines_fig.png)
=
##### Popularidad del Top 10 a lo largo de los años
![Motores gŕaficos por año](vis/engines_years_fig.png)
=
##### Popularidad de los lenguajes (audio y subtitulos)
![Soporte de lenguajes](vis/languages_fig.png)
=
##### Distribuciones de géneros y cateogrías de los videojuegos
![Géneros y categorías](vis/genres_categories_fig.png)
=
