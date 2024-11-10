import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv

def fetch_issuer_codes(url, csv_path):
    print("Fetching issuer codes...")
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        issuer_codes = soup.select('select[name="symbol"] option')

        with open(csv_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Issuer Code'])
            for code in issuer_codes:
                code_text = code.text.strip()
                if not any(char.isdigit() for char in code_text):
                    writer.writerow([code_text])
                    print(f"Issuer code {code_text} added to CSV.")
    else:
        print(f"Failed to fetch issuer codes. Status code: {response.status_code}")


def format_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        return date_str


def format_price(price_str):
    price_str = price_str.replace(',', '').replace(' ', '')
    try:
        price = float(price_str)
        return f"{price:,.2f}"
    except ValueError:
        return price_str



def data_exists(issuer_code, date_str, file_path):
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[0] == issuer_code and row[1] == date_str:
                return True
    return False



def fetch_data_for_multiple_years(issuer_code, file_path):
    print(f"Fetching data for {issuer_code} for the last 10 years.")

    end_date = datetime.now()
    start_date = datetime(end_date.year, 1, 1)


    while start_date.year >= (end_date.year - 10):
        start_date_str = start_date.strftime('%d.%m.%Y')
        end_date_str = (start_date + timedelta(days=365)).strftime('%d.%m.%Y')

        if datetime.strptime(end_date_str, '%d.%m.%Y') > end_date:
            end_date_str = end_date.strftime('%d.%m.%Y')

        print(f"Fetching data for {issuer_code} from {start_date_str} to {end_date_str}")

        url = f"https://www.mse.mk/mk/stats/symbolhistory/{issuer_code}"
        payload = {
            'fromDate': start_date_str,
            'toDate': end_date_str,
            'symbol': issuer_code,
            'action': 'fetchData',
        }

        response = requests.post(url, data=payload)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'class': 'table'})

            if table:
                rows = table.find_all('tr')
                data = []
                for row in rows[1:]:
                    columns = row.find_all('td')
                    if len(columns) > 3:
                        date = format_date(columns[0].text.strip())
                        price = format_price(columns[1].text.strip())
                        volume = columns[2].text.strip()
                        change = columns[3].text.strip()


                        if not data_exists(issuer_code, date, file_path):
                            data.append([issuer_code, date, price, volume, change])
                            print(f"New data for {issuer_code} on {date} added.")
                        else:
                            print(f"Data for {issuer_code} on {date} already exists. Skipping.")


                if data:
                    with open(file_path, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        if file.tell() == 0:
                            writer.writerow(['Issuer Code', 'Date', 'Price', 'Volume', 'Change'])
                        writer.writerows(data)
                        print(f"Data for {issuer_code} written to {file_path}.")
            else:
                print(
                    f"No table found for {issuer_code} from {start_date_str} to {end_date_str}. Response content: {response.content[:500]}")
        else:
            print(f"Failed to fetch data for {issuer_code}. HTTP Status code: {response.status_code}")


        start_date = start_date.replace(year=start_date.year - 1)



def update_missing_data(issuer_code, file_path):
    print(f"Checking for missing data for {issuer_code}...")


    existing_data = []
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[0] == issuer_code:
                existing_data.append(row)

    if existing_data:
        last_entry = existing_data[-1]
        last_date_str = last_entry[1]
        last_date = datetime.strptime(last_date_str, '%Y-%m-%d')


        if last_date < datetime.now():
            fetch_data_for_multiple_years(issuer_code, file_path)


def process_issuer_codes(csv_path, output_csv):
    print("Processing issuer codes and fetching data...")
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            issuer_code = row[0]
            update_missing_data(issuer_code, output_csv)
            fetch_data_for_multiple_years(issuer_code, output_csv)


def main():
    issuer_codes_csv = 'issuer_codes.csv'
    ten_years_data_csv = '10years_data.csv'

    fetch_issuer_codes('https://www.mse.mk/mk/stats/symbolhistory', issuer_codes_csv)

    process_issuer_codes(issuer_codes_csv, ten_years_data_csv)


if __name__ == "__main__":
    main()