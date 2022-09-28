import asyncio
import os
from typing import Optional
from uuid import uuid4

from aiohttp import web, web_request
from async_generator import async_generator, yield_

from src.file_creator import FileCreator
from src.process_service import ProcessService


@async_generator
async def file_sender(file_path: Optional[str] = None):
    """
    Reads large file chunk by chunk and sends it through HTTP
    without reading it into memory
    """
    with open(file_path, 'rb') as f:
        chunk = f.read(2**16)
        while chunk:
            await yield_(chunk)
            chunk = f.read(2**16)


async def create_input(request: web_request.Request):
    """
    Creates input csv files with provided parameters
    """
    data = await request.json()
    FileCreator.create_csv(
        name=data['name'],
        rows_number=data['rows_number'],
        dates_number=data['dates_number'],
        songs_list=data['songs_list'],
    )


async def process(request: web_request.Request):
    """
    Saves file by chunks without reading it into memory
    starts task that processes the provided file
    returns generated task id for user to recieve the output file later
    """
    reader = await request.multipart()
    field = await reader.next()
    assert field.name == 'file'

    task_id = uuid4()
    await ProcessService.save_file(field, task_id)
    task = asyncio.create_task(ProcessService.process_csv(task_id))
    task.set_name(task_id)
    return web.json_response({'task': str(task_id)})


async def download(request: web_request.Request):
    """
    Returns the output file by the task id, provided by user
    """
    data = await request.json()
    task_name = data['task']
    file_path = os.path.join(os.path.dirname(
        __file__), 'files', f'output_{task_name}.csv')
    if os.path.exists(file_path):
        headers = {
            "Content-disposition": "attachment; filename={file_name}".format(file_name=file_path)}
        return web.Response(body=file_sender(file_path=file_path), headers=headers)
    else:
        return web.Response(text='not yet')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    app = web.Application()
    app.add_routes(
        [
            web.post('/process', process),
            web.get('/download', download),
            web.post('/create-input', create_input),
        ]
    )

    web.run_app(app)
    loop.close()
