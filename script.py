import xml.etree.ElementTree as ET
import re
import sqlite3
import sys
from datetime import datetime


digit_regex = re.compile(r'\D')

def process_and_validate_ogrn(ogrn):
    """Remove all symbols besides digits
        and check length"""
    ogrn = re.sub(digit_regex, '', ogrn)
    return ogrn if len(ogrn) == 13 else None


def process_and_validate_inn(inn):
    """Remove all symbols besides digits
        and check length"""
    inn = re.sub(digit_regex, '', inn)
    return inn if len(inn) == 10 else None


def create_database():
    conn = sqlite3.connect('companies.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        ogrn TEXT NOT NULL UNIQUE,
        inn TEXT NOT NULL,
        name TEXT,
        phone TEXT,
        date DATETIME NOT NULL
    )
    ''')
    conn.commit()
    return conn


def process_companies(root):
    companies = {}
    # iterating over companies
    for company in root.findall('КОМПАНИЯ'):
        ogrn = company.find('ОГРН').text if company.find('ОГРН') is not None else None
        inn = company.find('ИНН').text if company.find('ИНН') is not None else None
        name = company.find('НазваниеКомпании').text if company.find('НазваниеКомпании') is not None else None
        phone = ', '.join(ph.text for ph in company.findall('Телефон')) \
            if company.find('Телефон') is not None else None
        date = datetime.strptime(company.find('ДатаОбн').text, '%Y-%m-%d') \
            if company.find('ДатаОбн') is not None else None

        if not process_and_validate_ogrn(ogrn):
            print(f'Некорректный ОГРН: {ogrn}')
            continue
        if not process_and_validate_inn(inn):
            print(f'Некорректный ИНН: {inn}')
            continue

        ogrn = process_and_validate_ogrn(ogrn)
        inn = process_and_validate_inn(inn)

        if ogrn not in companies:
            companies[ogrn] = {'inn': inn, 'name': name, 'phone': phone, 'date': date}
        else:
            print(f'Дубликат ОГРН: {ogrn},', end=' ')
            if date > companies[ogrn]['date']:
                companies[ogrn] = {'inn': inn, 'name': name, 'phone': phone, 'date': date}
                print('обновление данных')
            else:
                print('обновление данных не требуется')

    return companies


def insert_companies(conn, companies):
    cursor = conn.cursor()
    # preparing data to insert
    data_to_insert = [
        (ogrn, company['inn'], company['name'], company['phone'], company['date'].isoformat())
        for ogrn, company in companies.items()
    ]

    # INSERT SQL-query
    insert_query = '''
    INSERT OR REPLACE INTO companies (ogrn, inn, name, phone, date)
    VALUES (?, ?, ?, ?, ?)
    '''

    # inserting all data at once
    cursor.executemany(insert_query, data_to_insert)

    # save and close
    conn.commit()
    conn.close()

    print('Данные успешно записаны в базу')


def main():
    # getting path to XML
    if len(sys.argv) < 2:
        print("Пожалуйста, укажите путь к XML-файлу")
        sys.exit(1)
    file = sys.argv[1]

    # getting root and processing
    root = ET.parse(file).getroot()
    companies = process_companies(root)
    conn = create_database()
    insert_companies(conn, companies)


if __name__ == '__main__':
    main()







