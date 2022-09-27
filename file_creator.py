import csv
import random
import datetime
from typing import List


class FileCreator:

    def create_csv(name: str, rows_number: int, dates_number: int, songs_list: List[str]) -> None:
        songs_number = len(songs_list)

        base = datetime.datetime.today()
        date_list = [(base - datetime.timedelta(days=x)).strftime("%Y-%m-%d")
                     for x in range(dates_number)]
        with open(name, mode='w') as csv_file:
            fieldnames = ['Song', 'Date', 'Number of Plays']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for _ in range(rows_number):
                writer.writerow(
                    {'Song': songs_list[random.randint(0, songs_number - 1)],
                     'Date': date_list[random.randint(0, dates_number - 1)],
                     'Number of Plays': random.randint(0, 2000)})
