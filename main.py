from config import config
from src.parsing_hh import GetApiHh, JsonSaver, Vacancy, DBManager


def main():
    response = GetApiHh()

    # Get vacancies for user and info about employer
    vacancy_name = input("Enter name of vacancy for search: ")

    response.get_vacancy_from_api(vacancy_name)
    vacancies_list = response.all_vacancy

    # Get clean vacancies list
    clean_vacancies_list = Vacancy.clean_vacancy_list(vacancies_list)

    # Get employers id
    employers_id = []

    for info in clean_vacancies_list:
        employers_id.append(info.get('Employer id'))

    # Get info about employers
    employers_info = response.get_employers_info(set(employers_id))

    print("I get all info!")

    # Create database
    params = config()
    database = DBManager(params)

    users_db = input("Input name of database: ")

    print(database.create_data_base(users_db))

    # Create tables
    print(database.create_tables(users_db))

    # Save info about vacancies and employers in database
    print(database.save_data_to_database(clean_vacancies_list, employers_info, users_db))

    vacancies_count = int(input("How many vacancies show for you: "))

    # Get info about companies and their count of vacancies
    for row in database.get_companies_and_vacancies_count(users_db)[:vacancies_count]:
        print(f"Company '{row[0]}':\nCount vacancies: {row[1]}\n")

    # Get info about vacancies
    for row in database.get_all_vacancies('vacancies')[:vacancies_count]:
        print(f"Company: {row[0]}\nVacancy: {row[1]}\nSalary: from {row[2]} to {row[3]}\nURL: {row[4]}\n")

    # Get average salary
    avg_salary = 0
    for i in database.get_avg_salary(users_db)[:vacancies_count]:
        avg_salary = i
        print(f"Average salary for this vacancy: {avg_salary}\n")

    # Get all vacancies from DB which salary more than average salary
    for row in database.get_vacancies_with_higher_salary(users_db, avg_salary)[:vacancies_count]:
        print(f"Company: {row[0]}\nVacancy: {row[1]}\nSalary: from {row[2]} to {row[3]}\nURL: {row[4]}\n")

    # Get vacancies for keyword
    input_keyword = 'оператор'

    for row in database.get_vacancies_with_keyword(users_db, input_keyword)[:vacancies_count]:
        Vacancy.get_vacancy_list([{'Name vacancy': row[0], 'Salary from': row[1], 'Salary to': row[2],
                                   'Employer id': row[3], 'URL': row[4], 'City': row[5], 'Experience': row[6]}])

    for vacancy in Vacancy.list_vacancies:
        print(vacancy)


if __name__ == '__main__':
    main()
