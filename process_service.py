from asyncore import read
from collections import defaultdict
import asyncio
import aiofiles
from aiocsv import AsyncReader, AsyncDictWriter
from aiohttp.multipart import MultipartReader


class ProcessService:

    async def save_file(field: MultipartReader, task_id: str) -> None:
        async with aiofiles.open(f'input_{task_id}.csv', 'w') as f:
            while True:
                chunk = await field.read_chunk()
                if not chunk:
                    break
                await f.write(chunk.decode('UTF-8'))

    async def process_csv(task_id: str) -> None:
        res = defaultdict(int)
        async with aiofiles.open(f'input_{task_id}.csv', mode="r", encoding="utf-8", newline="\r\n") as afp:
            is_header = True
            async for row in AsyncReader(afp):
                if is_header:
                    is_header = False
                    continue
                res[(row[0], row[1])] += int(row[2])

        async with aiofiles.open(f'output_{task_id}.csv', mode="w", encoding="utf-8", newline="") as afp:
            fieldnames = ['Song', 'Date', 'Total Number of Plays for Date']
            writer = AsyncDictWriter(afp, fieldnames, restval="NULL")
            await writer.writeheader()
            for k, v in res.items():
                await writer.writerow(
                    {'Song': k[0], 'Date': k[1], 'Total Number of Plays for Date': v})

    @staticmethod
    def _get_output_file_name() -> str:
        return 'output_' + str(asyncio.current_task().get_name()) + '.csv'
