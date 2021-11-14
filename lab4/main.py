import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb',
                        password='adb2020')


def film_in_category(category_id: int) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0 |Amadeus Holy |English |Action|

    Tabela wynikowa ma być posortowana po tylule filmu i języku.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    q = f'''select film.title, language.name as language, category.name as category from category join film_category on category.category_id = film_category.category_id join film on film_category.film_id=film.film_id join language on film.language_id = language.language_id where category.category_id = {category_id} order by film.title, category.name'''

    if isinstance(category_id, int):
        return pd.read_sql(q, con=connection)
    else:
        return None



def number_films_in_category(category_id: int) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0 |Action |64  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    q = f'''select category.name, count(film.title) from category
        join film_category on category.category_id = film_category.category_id
        join film on film_category.film_id = film.film_id
        where category.category_id = {category_id} group by category.name
        '''
    if isinstance(category_id, int):
        return pd.read_sql(q, con=connection)
    else:
        return None



def number_film_by_length(min_length: Union[int, float] = 0, max_length: Union[int, float] = 1e6):
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0 |46    |64  |

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    q = f'''select film.length, count(film.title) from film
        where film.length >= {min_length} and film.length <= {max_length} group by film.length
        '''

    if isinstance(min_length, (int, float)) and isinstance(max_length, (int, float)):
        return pd.read_sql(q, con=connection)
    else:
        return None



def client_from_city(city: str) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city    |first_name |last_name
    |0 |Athenai |Linda    |Williams

    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    q = f"""select city.city, customer.first_name, customer.last_name from customer
        join address on customer.address_id = address.address_id
        join city on address.city_id = city.city_id
        where city.city = '{city}'"""

    if isinstance(city, str):
        return pd.read_sql(q, con=connection)
    else:
        return None

def avg_amount_by_length(length: Union[int, float]) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0 |48    |4.295389


    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    q = f'''select film.length, avg(payment.amount) from film
        join inventory on inventory.film_id=film.film_id
        join rental on rental.inventory_id = inventory.inventory_id
        join payment on payment.rental_id = rental.rental_id
        where film.length = {length} group by film.length
        '''

    if isinstance(length, (int, float)):
        return pd.read_sql(q, con=connection)
    else:
        return None



def client_by_sum_length(sum_min: Union[int, float]) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian    |Wyman   |1265

    Tabela wynikowa powinna być posortowane według sumy, nazwiska i imienia klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    q = f'''select customer.first_name, customer.last_name, sum(length) from film
        join inventory on inventory.film_id=film.film_id
        join rental on rental.inventory_id=inventory.inventory_id
        join customer on customer.customer_id=rental.customer_id
        group by customer.first_name, customer.last_name
        having sum(film.length) > {sum_min}
        order by sum, customer.last_name, customer.first_name
        '''

    if isinstance(sum_min, (int, float)) or sum_min >= 0:
        return pd.read_sql_query(q, con=connection)
    else:
        return None



def category_statistic_length(name: str) -> pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0 |Action |111.60 |7143   |47 |185

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.

    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka

    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    q = f'''select category.name, avg(film.length), sum(film.length), min(film.length), max(film.length) from film
        join film_category on film_category.film_id = film.film_id
        join category on category.category_id = film_category.category_id
        where category.name = '{name}' group by category.name 
        '''

    if isinstance(name, str):
        return pd.read_sql_query(q, con=connection)
    else:
        return None
