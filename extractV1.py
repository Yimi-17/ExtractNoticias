import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import mysql.connector

# Define las credenciales de la base de datos
db_config = {
    'user': 'hvbamh8l4zxi5j71t1s5',
    'password': 'pscale_pw_XbICKp2cIJQ5UDPNzEPFpnVYY85SqpVOJl3vhZaE5Yb',
    'host': 'aws.connect.psdb.cloud',
    'database': 'test1db'
}

# Define la fecha de inicio y fin
# fecha_inicio = datetime(2018, 11, 7)
fecha_inicio = datetime(2018, 11, 7)
fecha_fin = datetime(2023, 9, 5)

# URL base
base_url = "https://www.losandes.com.pe/"

# Función para generar las URLs de las fechas
def generar_url(fecha):
    return f"{base_url}{fecha.year}/{fecha.strftime('%m/%d')}/"

# Conecta a la base de datos
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Crear la tabla 'noticiascr' si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS noticiascr (
        id INT AUTO_INCREMENT PRIMARY KEY,
        fecha DATE,
        titulo VARCHAR(255),
        url VARCHAR(255),
        imagen VARCHAR(255),
        entrada TEXT,
        categoria VARCHAR(255),
        vistas VARCHAR(255)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Realiza un bucle para recorrer las fechas y raspar los datos
    current_date = fecha_inicio
    while current_date <= fecha_fin:
        url = generar_url(current_date)

        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            content_wrapper = soup.find("div", class_="jnews_archive_content_wrapper")
            articles = content_wrapper.find_all("article")

            for article in articles:
                h3_element = article.find("h3")
                if h3_element:
                    a_element = h3_element.find("a")
                    if a_element:
                        titulo = a_element.text.strip()
                        url_noticia = a_element.get("href")
                    else:
                        titulo = h3_element.text.strip()
                        url_noticia = 'N/A'

                    img_element = article.find("img")
                    if img_element:
                        img_url = img_element.get("src")
                    else:
                        img_url = 'N/A'

                    p_element = article.find("p")
                    if p_element:
                        parrafo = p_element.text.strip()
                    else:
                        parrafo = 'N/A'

                    category_div = article.find("div", class_="jeg_post_category")
                    if category_div:
                        categoria = category_div.text.strip()
                    else:
                        categoria = 'N/A'

                    views_div = article.find("div", class_="jeg_meta_views")
                    if views_div:
                        views = views_div.text.strip()
                    else:
                        views = 'N/A'

                    # Insertar los datos en la tabla 'noticiascr'
                    insert_query = """
                    INSERT INTO noticiascr (fecha, titulo, url, imagen, entrada, categoria, vistas)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    insert_data = (current_date.strftime('%Y-%m-%d'), titulo, url_noticia, img_url, parrafo, categoria, views)
                    cursor.execute(insert_query, insert_data)
                    conn.commit()
        else:
            print("======================================")
            print(f"Fecha: {current_date.strftime('%Y-%m-%d')}")
            print(f"{response.status_code}")
            print("======================================")

        # Avanza a la siguiente fecha
        current_date += timedelta(days=1)

    print("Datos insertados correctamente en la tabla 'noticiascr'.")

except mysql.connector.Error as error:
    print("Error en la conexión a la base de datos:", error)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()

print("Proceso completado.")
