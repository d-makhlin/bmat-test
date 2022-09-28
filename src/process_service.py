import asyncio
import os
from collections import defaultdict
from typing import Dict

import aiofiles
import aioredis
from aiocsv import AsyncDictWriter, AsyncReader
from aiohttp.multipart import MultipartReader


class ProcessService:
    async def save_file(field: MultipartReader, task_id: str) -> None:
        """
        Saves file by chunks with multipart
        """
        path = os.path.join(os.path.dirname(__file__), '..',
                            'files', f'input_{task_id}.csv')
        async with aiofiles.open(path, 'w') as f:
            while True:
                chunk = await field.read_chunk()
                if not chunk:
                    break
                await f.write(chunk.decode('UTF-8'))

    async def process_csv(task_id: str) -> None:
        """
        Asynchronously reads data from saved csv file line by line
        Uses defaultdict as a temporary kay value storage
        Stores data in redis asynchronously when the dict becomes large
        """
        path = os.path.join(os.path.dirname(__file__), '..',
                            'files', f'input_{task_id}.csv')

        r_host = os.environ.get('REDIS_HOST', 'redis')
        # connect to redis
        redis = await aioredis.from_url(f'{r_host}://', port=6379)

        data = defaultdict(int)
        async with aiofiles.open(path, mode="r", encoding="utf-8", newline="\r\n") as afp:
            is_header = True
            async for row in AsyncReader(afp):
                if is_header:  # skip header when read lines
                    is_header = False
                    continue

                # construct key by merging the songs name and date, add the playing number
                data[str(row[0]) + ':' + str(row[1])] += int(row[2])

                if len(data) > 100_000:  # If the dict is large, put data into redis and clean it
                    await ProcessService._store_data_in_redis(data)
                    data = {}

        # write stored in redis data into file asynchronously
        async with aiofiles.open(ProcessService._get_output_file_name(), mode="w", encoding="utf-8", newline="") as afp:
            fieldnames = ['Song', 'Date', 'Total Number of Plays for Date']
            writer = AsyncDictWriter(afp, fieldnames, restval="NULL")
            await writer.writeheader()
            async for k in redis.scan_iter('*'):
                song_and_date_list = k.decode('UTF-8').split(':')
                number = await redis.get(k)
                await writer.writerow(
                    {
                        'Song': song_and_date_list[0],
                        'Date': song_and_date_list[1],
                        'Total Number of Plays for Date': number.decode('UTF-8'),
                    }
                )

        await redis.close()

    @staticmethod
    def _get_output_file_name() -> str:
        return os.path.join(
            os.path.dirname(
                __file__), '..', 'files', f'output_{str(asyncio.current_task().get_name())}.csv'
        )

    async def _store_data_in_redis(conn: aioredis.Redis, data: Dict[str, int]) -> None:
        """
        Puts data from python dict to redis
        """
        for k, v in data.items():
            old_v = await conn.exists(k)
            old_v = int(v) if v else 0
            await conn.set(k, v + old_v)
