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

 ![Motores gŕaficos](vis/engines_fig.png)
