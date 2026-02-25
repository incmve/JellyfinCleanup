import json
import os
import logging
import requests
from datetime import datetime
from flask import Flask, send_file, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

DATA_DIR = '/data'
SCHEDULE_FILE = os.path.join(DATA_DIR, 'schedule.json')
HISTORY_FILE  = os.path.join(DATA_DIR, 'history.json')
os.makedirs(DATA_DIR, exist_ok=True)

scheduler = BackgroundScheduler()
scheduler.start()

# ── Helpers ───────────────────────────────────────────────

def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def add_history(entry):
    hist = load_json(HISTORY_FILE, [])
    hist.insert(0, entry)
    if len(hist) > 50:
        hist = hist[:50]
    save_json(HISTORY_FILE, hist)

# ── Cleanup job ───────────────────────────────────────────

def run_cleanup(dry_run=False):
    cfg = load_json(SCHEDULE_FILE, {})
    server_url = cfg.get('serverUrl', '').rstrip('/')
    api_key    = cfg.get('apiKey', '')

    if not server_url or not api_key:
        log.warning('Cleanup skipped — no server URL or API key configured.')
        return

    headers = {'X-Emby-Token': api_key, 'Content-Type': 'application/json'}
    label   = '[DRY RUN]' if dry_run else '[LIVE]'
    log.info(f'{label} Starting cleanup...')

    try:
        # Get all active users
        users_resp = requests.get(f'{server_url}/Users', headers=headers, timeout=10)
        users_resp.raise_for_status()
        users = [u for u in users_resp.json() if not u.get('Policy', {}).get('IsDisabled')]
        log.info(f'Found {len(users)} active users')

        # Get all movies
        movies_resp = requests.get(
            f'{server_url}/Items',
            headers=headers,
            params={'IncludeItemTypes': 'Movie', 'Recursive': 'true', 'Fields': 'MediaSources', 'Limit': 5000},
            timeout=30
        )
        movies_resp.raise_for_status()
        movies = movies_resp.json().get('Items', [])
        log.info(f'Found {len(movies)} movies')

        # Build watched set per movie
        watch_data = {m['Id']: set() for m in movies}
        for user in users:
            watched_resp = requests.get(
                f'{server_url}/Items',
                headers=headers,
                params={'userId': user['Id'], 'IncludeItemTypes': 'Movie', 'Recursive': 'true', 'IsPlayed': 'true', 'Fields': 'Id', 'Limit': 5000},
                timeout=30
            )
            watched_resp.raise_for_status()
            for m in watched_resp.json().get('Items', []):
                if m['Id'] in watch_data:
                    watch_data[m['Id']].add(user['Id'])

        # Find eligible movies (all users watched)
        eligible = [m for m in movies if len(watch_data.get(m['Id'], set())) == len(users) and len(users) > 0]
        log.info(f'Found {len(eligible)} movies watched by all users')

        deleted, failed, names = 0, 0, []
        for movie in eligible:
            if dry_run:
                log.info(f'[DRY RUN] Would delete: {movie["Name"]}')
                names.append(movie['Name'])
                deleted += 1
            else:
                try:
                    r = requests.delete(f'{server_url}/Items/{movie["Id"]}', headers=headers, timeout=10)
                    r.raise_for_status()
                    log.info(f'Deleted: {movie["Name"]}')
                    names.append(movie['Name'])
                    deleted += 1
                except Exception as e:
                    log.error(f'Failed to delete {movie["Name"]}: {e}')
                    failed += 1

        entry = {
            'time':    datetime.now().isoformat(),
            'deleted': deleted,
            'failed':  failed,
            'dryRun':  dry_run,
            'names':   names
        }
        add_history(entry)
        log.info(f'{label} Done. {deleted} deleted, {failed} failed.')

    except Exception as e:
        log.error(f'Cleanup error: {e}')
        add_history({'time': datetime.now().isoformat(), 'deleted': 0, 'failed': 0, 'dryRun': dry_run, 'names': [], 'error': str(e)})

# ── Scheduler ─────────────────────────────────────────────

def apply_schedule(cfg):
    scheduler.remove_all_jobs()
    if not cfg.get('enabled'):
        log.info('Schedule disabled.')
        return

    freq = cfg.get('freq', 'daily')
    hour = int(cfg.get('time', 3))
    day  = int(cfg.get('day', 3))   # 0=Sun … 6=Sat
    dry_run = cfg.get('dryRun', False)

    # APScheduler uses 0=Mon … 6=Sun for day_of_week, so convert
    dow = (day - 1) % 7  # Sun(0)->6, Mon(1)->0, …

    if freq == 'hourly':
        trigger = CronTrigger(minute=0)
    elif freq == 'daily':
        trigger = CronTrigger(hour=hour, minute=0)
    elif freq == 'weekly':
        trigger = CronTrigger(day_of_week=dow, hour=hour, minute=0)
    elif freq == 'monthly':
        trigger = CronTrigger(day=1, hour=hour, minute=0)
    else:
        return

    scheduler.add_job(run_cleanup, trigger, kwargs={'dry_run': dry_run}, id='cleanup', replace_existing=True)
    log.info(f'Schedule set: {freq} (dry_run={dry_run})')

# Load saved schedule on startup
apply_schedule(load_json(SCHEDULE_FILE, {}))

# ── Routes ────────────────────────────────────────────────

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/api/schedule', methods=['GET'])
def get_schedule():
    return jsonify(load_json(SCHEDULE_FILE, {}))

@app.route('/api/schedule', methods=['POST'])
def set_schedule():
    cfg = request.get_json()
    save_json(SCHEDULE_FILE, cfg)
    apply_schedule(cfg)
    return jsonify({'ok': True})

@app.route('/api/run', methods=['POST'])
def run_now():
    dry_run = request.get_json(silent=True, force=True) or {}
    dry_run = dry_run.get('dryRun', False)
    import threading
    threading.Thread(target=run_cleanup, kwargs={'dry_run': dry_run}, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/history', methods=['GET'])
def get_history():
    return jsonify(load_json(HISTORY_FILE, []))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
