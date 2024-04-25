import json
from abc import ABC, abstractmethod
from typing import Any

import psycopg2
import requests

from config import DATA
from config import config


class AbstractGetApiHh(ABC):

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def get_vacancy_from_api(self, name_vacancy):
        pass


class GetApiHh(AbstractGetApiHh):

    def __init__(self):
        self.all_vacancy = []
        self.employer_info = []

    def __repr__(self):
        return f"{self.all_vacancy}"

    def get_vacancy_from_api(self, name_vacancy) -> list:
        """Get valid info about vacancies for user"""
        keys_response = {'text': {name_vacancy}, 'area': 113, 'per_page': 100, }
        info = requests.get(f'https://api.hh.ru/vacancies', keys_response)
        self.all_vacancy = json.loads(info.text)['items']
        return self.all_vacancy

    def get_employers_info(self, employers_id):
        """For in employers_id and get list with info about employers"""
        employers_info = []
        for id_employer in employers_id:
            employer_info = self.get_info_about_employer(id_employer)
            employers_info.append({'Employer id': employer_info['id'], 'Employer name': employer_info['name'],
                                   'Employer description': employer_info['alternate_url'],
                                   'Employer vacancies': employer_info['vacancies_url'],
                                   'Open vacancies': employer_info['open_vacancies']})
        return employers_info

    def get_info_about_employer(self, employer_id: int) -> list:
        """Get valid info about employer for user"""
        info = requests.get(f'https://api.hh.ru/employers/{employer_id}')
        self.employer_info = json.loads(info.text)
        return self.employer_info


class AbstractJsonSaver(ABC):

    @abstractmethod
    def save_file(self, data: list):
        pass

    @abstractmethod
    def read_file(self):
        pass


class JsonSaver(AbstractJsonSaver):

    def save_file(self, data: list):
        """Save file"""
        with open(DATA, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, indent=2, ensure_ascii=False))

    def read_file(self):
        """Read file"""
        with open(DATA, encoding='utf-8') as file:
            return json.load(file)

    def add_vacancy_to_file(self, data: list):
        old_list = self.read_file()
        new_list = data + old_list
        self.save_file(new_list)

    def delete_vacancy(self, vacancy: str):
        new_list = []

        old_list = self.read_file()

        for params in old_list:
            if params['name'] != vacancy:
                new_list.append(params)

        self.save_file(new_list)


class Vacancy:
    list_vacancies = []

    def __init__(self, name_vacancy: str, salary_from: int, salary_to: int, employer_id: int,
                 url: str, city: str, experience: str):
        self.name_vacancy = name_vacancy
        self.salary_from = salary_from
        self.salary_to = salary_to
        self.employer_id = employer_id
        self.url = url
        self.city = city
        self.experience = experience

    def __repr__(self):
        return (f"Vacancy name: {self.name_vacancy}\n"
                f"Salary from: {self.salary_from}\n"
                f"Salary to: {self.salary_to}\n"
                f"Employer id: {self.employer_id}\n"
                f"URL: {self.url}\n"
                f"City: {self.city}\n"
                f"Experience: {self.experience}\n")

    def __lt__(self, other):
        if other.salary_to < self.salary_to:
            return True

    @staticmethod
    def clean_vacancy_list(list_vacancy: list) -> list:
        """
        Sorted list arguments.
        :param list_vacancy: list with info about vacancies
        :return: new sorted list
        """
        new_list = list()

        for vacancy_info in list_vacancy:
            if vacancy_info["salary"] is not None and vacancy_info["salary"]["from"] is not None:
                if vacancy_info["salary"]["to"] is None:
                    vacancy_info["salary"]["to"] = vacancy_info["salary"]["from"]
                new_list.append(
                    {'Name vacancy': vacancy_info["name"], 'Salary from': vacancy_info["salary"]["from"],
                     'Salary to': vacancy_info["salary"]["to"], 'Employer id': vacancy_info["employer"]["id"],
                     'URL': vacancy_info["alternate_url"],
                     'City': vacancy_info["area"]["name"], 'Experience': vacancy_info["experience"]["name"]})
            else:
                continue
        return new_list

    @classmethod
    def get_vacancy_list(cls, list_vacancy) -> list:
        """
        Get list with vacancies dicts. This list with copy of class Vacancy
        :return: new lisrt with copy of class Vacancy
        """
        for vacancy in list_vacancy:
            cls.list_vacancies.append([(cls(vacancy['Name vacancy'], vacancy['Salary from'], vacancy['Salary to'],
                                            vacancy['Employer id'], vacancy['URL'],
                                            vacancy['City'], vacancy['Experience']))])
        return cls.list_vacancies


class DBManager:

    def __init__(self, params: dict):
        self.params = params

    def create_data_base(self, name_db) -> None:
        """
        Create database and tables.
        :param name_db: name of database
        """
        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True

        cur = conn.cursor()

        cur.execute(f'DROP DATABASE {name_db}')
        cur.execute(f'CREATE DATABASE {name_db}')

        return "Database created"

    def create_tables(self, name_db: str) -> None:
        """
        Create table for database.
        :param name_db: name of database
        """

        with psycopg2.connect(dbname=name_db, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE info_vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                name_vacancy VARCHAR(255) NOT NULL,
                salary_from INTEGER,
                salary_to INTEGER,
                employer_id INTEGER,
                url TEXT,
                city VARCHAR(100),
                experience TEXT
            )
            """)
                cur.execute("""
                CREATE TABLE info_employers (
                employer_id INTEGER,
                company_name VARCHAR(255),
                description text,
                vacancies_url TEXT,
                open_vacancies INTEGER,
                CONSTRAINT pk_info_employers_employer_id PRIMARY KEY (employer_id)
                )
            """)
        conn.close()
        return "Tables created"

    def save_data_to_database(self, vacancies_file: list, company_data: list,
                              name_bd: str) -> None:
        """
        Save data info about vacancies.
        :param vacancies_file: info about vacancies for save
        :param company_data: info about employers for save
        :param name_bd: name of database
        """

        conn = psycopg2.connect(dbname=name_bd, **self.params)
        conn.autocommit = True
        with conn.cursor() as cur:
            for data in vacancies_file:
                count_columns = '%s ' * len(data)
                val = tuple(data.values())
                cur.execute(f"INSERT INTO info_vacancies (name_vacancy, salary_from, salary_to,"
                            f"employer_id, url, city, experience) "
                            f"VALUES({', '.join(count_columns.split())})", val)

            for data in company_data:
                count_columns = '%s ' * len(data)
                val = tuple(data.values())
                cur.execute(f"INSERT INTO info_employers (employer_id, company_name, description,"
                            f"vacancies_url, open_vacancies)"
                            f"VALUES({', '.join(count_columns.split())})", val)
        conn.close()
        return f"Info about vacancies save in database: {name_bd} in tables."

    def get_companies_and_vacancies_count(self, name_bd: str) -> list:
        """
        Get list of companies and count of vacancies
        :param name_bd: name of database
        :return: a list with tuple with info about company name
        and count vacancies
        """
        with psycopg2.connect(dbname=name_bd, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT company_name, open_vacancies FROM info_employers
                                    ORDER BY open_vacancies DESC""")
                company_and_vacancies = cur.fetchall()
        conn.close()
        return company_and_vacancies[:10]

    def get_all_vacancies(self, name_bd: str) -> list:
        """
        Get all info about vacancies and name of company.
        :param name_bd: name of database
        :return: info about vacancies and name of company
        """

        with psycopg2.connect(dbname=name_bd, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT company_name, i.name_vacancy, i.salary_from, i.salary_to, i.url 
                            FROM info_employers
                            JOIN info_vacancies AS i USING(employer_id)
                            ORDER BY (i.salary_to+i.salary_from)/2 DESC""")

                vacancies_info = cur.fetchall()
        conn.close()
        return vacancies_info[:10]

    def get_avg_salary(self, name_bd: str):
        """
        Get average salary in this vacancy.
        :param name_bd: name of database
        :return: average salary
        """

        with psycopg2.connect(dbname=name_bd, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT AVG((salary_to+salary_from)/2) DESC FROM info_vacancies""")
                avg_salary = cur.fetchall()
        conn.close()
        salary_avg = [round(i) for salary in avg_salary for i in salary]
        return salary_avg

    def get_vacancies_with_higher_salary(self, name_bd: str, salary: int):
        """
        Get vacancies with salary from average salary.
        :param name_bd: name of database
        :param salary: average salary.
        :return: vacancies with salary from average salary.
        """
        with psycopg2.connect(dbname=name_bd, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, i.name_vacancy, i.salary_from, i.salary_to, i.url "
                            f"FROM info_employers "
                            f"JOIN info_vacancies AS i USING(employer_id)"
                            f"WHERE salary_from >= {salary} "
                            f"ORDER BY (i.salary_to+i.salary_from)/2 DESC")
                vacancies_info = cur.fetchall()
        conn.close()
        return vacancies_info

    def get_vacancies_with_keyword(self, name_bd: str, keyword: str):
        """
        Compare vacancies with keyword and get these for user/
        :param keyword: keyword for search
        :return: vacancies
        """
        with psycopg2.connect(dbname=name_bd, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT name_vacancy, salary_from, salary_to, employer_id,"
                            f"url, city, experience FROM info_vacancies "
                            f"WHERE name_vacancy LIKE '%{keyword}' "
                            f"OR name_vacancy LIKE '{keyword}%'")
                vacancies_info = cur.fetchall()
        conn.close()
        return vacancies_info
