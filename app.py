from flask import Flask, request, render_template, redirect, url_for, jsonify
import os
from redis import Redis
from rq import Queue
from tasks import run_optimization_job

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('outputs', exist_ok=True)

# Connect to Redis
# On Render.com, you'll set the REDIS_URL environment variable
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_conn = Redis.from_url(redis_url)
q = Queue(connection=redis_conn)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/run', methods=['POST'])
def run_task():
    # Handle file upload
    file = request.files['data_file']
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    # Get other form data
    api_key = request.form['api_key']
    days_ahead = int(request.form['days_ahead'])

    # Enqueue the job
    job = q.enqueue(run_optimization_job, filepath, api_key, days_ahead, job_timeout='10m')

    # Redirect to the results page
    return redirect(url_for('results', job_id=job.id))


@app.route('/results/<job_id>')
def results(job_id):
    return render_template('results.html', job_id=job_id)


@app.route('/status/<job_id>')
def job_status(job_id):
    job = q.fetch_job(job_id)
    if job:
        response = {
            'status': job.get_status(),
            'result': job.result,
        }
        return jsonify(response)
    return jsonify({'status': 'not_found'})