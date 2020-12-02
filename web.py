

from urllib.parse import urlparse

from flask import Flask, request
from celery.result import AsyncResult

from tasks import do_parse, app as celery_app

app = Flask(__name__)


@app.route('/')
def index():
    return '''
    <form method="POST" action="/parse">
    <label for="url">URL: </label><input id="url" type="text" name="url"><input type="submit" value="Parse">
    </form>
    <br>
    '''


RESULT_SUCCESS = 0
RESULT_ERROR_PARAMS = 1


@app.route('/parse/', methods=['POST'])
def parse():
    url = request.form.get('url', '').strip()
    if not url:
        return {'result': RESULT_ERROR_PARAMS, 'errors': {'url': 'required'}}
    parsed_url = urlparse(url)
    if not parsed_url.netloc:
        return {'result': RESULT_ERROR_PARAMS, 'errors': {'url': 'invalid'}}
    result = do_parse.apply_async((url, ))
    tasks[result.id] = result
    return {'result': RESULT_SUCCESS, 'task_id': result.id}


@app.route('/parse/<string:task_id>', methods=['GET'])
def parse_task(task_id: str):
    result = AsyncResult(task_id, app=celery_app)
    ready = result.ready()
    if ready:
        return {'result': RESULT_SUCCESS, 'task_id': task_id, 'url': result.get()}
    else:
        return {'result': RESULT_SUCCESS, 'task_id': task_id, 'ready': False}


if __name__ == '__main__':
    app.run()
