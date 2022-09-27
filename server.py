import os
import asyncio
from uuid import uuid4
from file_creator import FileCreator
from async_generator import async_generator, yield_
from process_service import ProcessService
from aiohttp import web, web_request
from typing import Optional


@async_generator
async def file_sender(file_path: Optional[str] = None):
    """
    This function will read large file chunk by chunk and send it through HTTP
    without reading them into memory
    """
    with open(file_path, 'rb') as f:
        chunk = f.read(2 ** 16)
        while chunk:
            await yield_(chunk)
            chunk = f.read(2 ** 16)


async def create_input(request: web_request.Request):
    data = await request.json()
    FileCreator.create_csv(
        name=data['name'],
        rows_number=data['rows_number'],
        dates_number=data['dates_number'],
        songs_list=data['songs_list'],
    )


async def process(request: web_request.Request):
    reader = await request.multipart()
    field = await reader.next()
    assert field.name == 'file'

    task = asyncio.create_task(ProcessService.process_csv(field))
    task_name = uuid4()
    task.set_name(task_name)
    return web.json_response({'task': str(task_name)})


async def download(request: web_request.Request):
    data = await request.json()
    task_name = data['task']
    file_path = f'output_{task_name}.csv'
    if os.path.exists(file_path):
        headers = {
            "Content-disposition": "attachment; filename={file_name}".format(file_name=file_path)
        }
        return web.Response(
            body=file_sender(file_path=file_path),
            headers=headers
        )
    else:
        return web.Response(text='not yet')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    app = web.Application()
    app.add_routes([
        web.post('/process', process),
        web.get('/download', download),
        web.post('/create-input', create_input),
    ])

    web.run_app(app)
    loop.close()
