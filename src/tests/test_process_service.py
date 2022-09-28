import os
import csv
import pytest
import datetime

from src.process_service import ProcessService


@pytest.mark.skip  # I would have finished it using mocks if I had a bit more time
@pytest.mark.asyncio
@pytest.mark.parametrize('dates_number, songs_number', ((1, 1), (10, 5), (100, 20)))
async def test_process_service(dates_number, songs_number) -> None:
    base = datetime.datetime.today()
    date_list = [(base - datetime.timedelta(days=x)).strftime("%Y-%m-%d")
                 for x in range(dates_number)]
    songs_list = [f'song_{i}' for i in range(songs_number)]
    path = os.path.join(os.path.dirname(__file__), '..', '..',
                        'files', f'input_test_{str(dates_number)}_{str(songs_number)}.csv')

    with open(path, mode='w') as csv_file:
        fieldnames = ['Song', 'Date', 'Number of Plays']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1000):
            writer.writerow(
                {
                    'Song': songs_list[i % songs_number],
                    'Date': date_list[i % dates_number],
                    'Number of Plays': 1,
                }
            )

        await ProcessService.process_csv(
            f'test_{str(dates_number)}_{str(songs_number)}')
