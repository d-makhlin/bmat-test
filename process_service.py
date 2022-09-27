from collections import defaultdict
import asyncio
import csv


class ProcessService:

    async def process_csv(field) -> None:
        res = defaultdict(int)
        await field.readline()
        while True:
            chunk = await field.readline()
            if not chunk:
                break
            chunk_data = str(chunk)[2:-5].split(',')
            res[(chunk_data[-3], chunk_data[-2])] += int(chunk_data[-1])

        with open(ProcessService._get_output_file_name(), mode='w') as csv_file:
            fieldnames = ['Song', 'Date', 'Total Number of Plays for Date']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for k, v in res.items():
                writer.writerow(
                    {'Song': k[0], 'Date': k[1], 'Total Number of Plays for Date': v})

    @staticmethod
    def _get_output_file_name() -> str:
        return 'output_' + str(asyncio.current_task().get_name()) + '.csv'
