import os
import re
import json
import uuid
import threading
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = Flask(__name__)
limiter = Limiter(app, key_func=get_remote_address)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.environ['SUPABASE_HOST'],
        database=os.environ['SUPABASE_DB'],
        user=os.environ['SUPABASE_USER'],
        password=os.environ['SUPABASE_PASSWORD'],
        cursor_factory=RealDictCursor
    )

# GeoIP lookup
def get_geoip_data(ip):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check cache first
    cur.execute("SELECT * FROM geoip_cache WHERE ip = %s AND expires_at > NOW()", (ip,))
    cached = cur.fetchone()
    
    if cached:
        cur.close()
        conn.close()
        return cached
    
    # Fetch from API
    try:
        response = requests.get(f'http://ip-api.com/json/{ip}?fields=status,message,country,countryCode,regionName,city,isp,lat,lon', timeout=5)
        data = response.json()
        
        if data['status'] == 'success':
            geoip_data = {
                'ip': ip,
                'country_code': data.get('countryCode'),
                'country_name': data.get('country'),
                'region': data.get('regionName'),
                'city': data.get('city'),
                'isp': data.get('isp'),
                'latitude': data.get('lat'),
                'longitude': data.get('lon'),
                'expires_at': datetime.now() + timedelta(days=30)
            }
            
            # Save to cache
            cur.execute("""
                INSERT INTO geoip_cache (ip, country_code, country_name, region, city, isp, latitude, longitude, expires_at)
                VALUES (%(ip)s, %(country_code)s, %(country_name)s, %(region)s, %(city)s, %(isp)s, %(latitude)s, %(longitude)s, %(expires_at)s)
                ON CONFLICT (ip) DO UPDATE SET
                    country_code = EXCLUDED.country_code,
                    country_name = EXCLUDED.country_name,
                    region = EXCLUDED.region,
                    city = EXCLUDED.city,
                    isp = EXCLUDED.isp,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    expires_at = EXCLUDED.expires_at
            """, geoip_data)
            conn.commit()
            
            cur.close()
            conn.close()
            return geoip_data
        else:
            cur.close()
            conn.close()
            return None
    except Exception as e:
        cur.close()
        conn.close()
        return None

# Test proxy
def test_proxy(proxy_url):
    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }
    
    try:
        response = requests.get(
            'http://httpbin.org/ip',
            proxies=proxies,
            timeout=10
        )
        return {
            'success': True,
            'response_time': response.elapsed.total_seconds(),
            'origin_ip': response.json().get('origin')
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

# Extract proxies from text
def extract_proxies_from_text(text):
    patterns = [
        r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}\b',
        r'\b[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}:[0-9]{1,5}\b',
        r'\b[\w.-]+@[\w.-]+:[0-9]{1,5}\b'
    ]
    
    proxies = set()
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            proxy = match.strip()
            if proxy and ':' in proxy:
                proxies.add(proxy)
    
    return list(proxies)

# Validate proxy format
def validate_proxy_format(proxy):
    pattern = r'^([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}$'
    return re.match(pattern, proxy) is not None

# Log system event
def log_system(level, message, component=None):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO system_logs (level, message, component)
        VALUES (%s, %s, %s)
    """, (level, message, component))
    
    conn.commit()
    cur.close()
    conn.close()

# Send Telegram notification
def send_telegram_message(message):
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if bot_token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            requests.post(url, json=data, timeout=5)
        except:
            pass

# API Endpoints
@app.route('/check', methods=['GET'])
def get_proxy_checks():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    country_code = request.args.get('cc')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = "SELECT * FROM proxy_results"
    params = []
    
    if country_code:
        query += " WHERE country_code = %s"
        params.append(country_code.upper())
    
    query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([per_page, (page - 1) * per_page])
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    # Get total count
    count_query = "SELECT COUNT(*) FROM proxy_results"
    count_params = []
    
    if country_code:
        count_query += " WHERE country_code = %s"
        count_params.append(country_code.upper())
    
    cur.execute(count_query, count_params)
    total = cur.fetchone()['count']
    
    cur.close()
    conn.close()
    
    return jsonify({
        'data': results,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        }
    })

@app.route('/stats', methods=['GET'])
def get_worker_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM workers ORDER BY last_ping DESC")
    workers = cur.fetchall()
    
    # Calculate uptime
    for worker in workers:
        if worker['last_ping']:
            uptime = datetime.now() - worker['last_ping']
            worker['uptime'] = str(uptime)
    
    cur.close()
    conn.close()
    
    return jsonify({
        'workers': workers,
        'summary': {
            'total_workers': len(workers),
            'active_workers': len([w for w in workers if w['status'] == 'idle'])
        }
    })

@app.route('/direct', methods=['POST'])
@limiter.limit("10 per minute")
def direct_check():
    data = request.get_json()
    proxies = []
    
    if isinstance(data.get('proxies'), str):
        proxies = [p.strip() for p in data['proxies'].split(',')]
    elif isinstance(data.get('proxies'), list):
        proxies = data['proxies']
    
    # Validate proxies
    valid_proxies = []
    for proxy in proxies:
        if validate_proxy_format(proxy):
            valid_proxies.append(proxy)
    
    if not valid_proxies:
        return jsonify({'error': 'No valid proxies found'}), 400
    
    # Create job
    job_id = str(uuid.uuid4())
    
    # Add to queue
    conn = get_db_connection()
    cur = conn.cursor()
    
    for proxy in valid_proxies:
        cur.execute("""
            INSERT INTO proxy_queue (job_id, proxy_url, status)
            VALUES (%s, %s, 'pending')
        """, (job_id, proxy))
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Notify Telegram
    send_telegram_message(f"🔍 *New Job Started*\nJob ID: `{job_id}`\nProxies: {len(valid_proxies)}")
    
    return jsonify({
        'message': f'{len(valid_proxies)} proxies queued',
        'job_id': job_id,
        'invalid_proxies': len(proxies) - len(valid_proxies)
    })

@app.route('/direct', methods=['GET'])
def direct_check_from_url():
    url = request.args.get('url')
    
    if not url:
        return jsonify({'error': 'URL parameter required'}), 400
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        proxies = extract_proxies_from_text(response.text)
        
        if not proxies:
            return jsonify({'error': 'No proxies found in URL'}), 400
        
        # Create job
        job_id = str(uuid.uuid4())
        
        # Add to queue
        conn = get_db_connection()
        cur = conn.cursor()
        
        for proxy in proxies:
            cur.execute("""
                INSERT INTO proxy_queue (job_id, proxy_url, status)
                VALUES (%s, %s, 'pending')
            """, (job_id, proxy))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Notify Telegram
        send_telegram_message(f"🌐 *URL Job Started*\nJob ID: `{job_id}`\nURL: {url}\nProxies: {len(proxies)}")
        
        return jsonify({
            'message': f'{len(proxies)} proxies extracted and queued',
            'job_id': job_id,
            'source_url': url
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/analyze', methods=['POST'])
def analyze_proxy_source():
    data = request.get_json()
    url = data.get('url')
    
    if not url:
        return jsonify({'error': 'URL required'}), 400
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        proxies = extract_proxies_from_text(response.text)
        
        # Analyze content
        lines = response.text.splitlines()
        proxy_lines = 0
        
        for line in lines:
            line = line.strip()
            if ':' in line and len(line.split(':')) == 2:
                try:
                    ip, port = line.split(':')
                    if re.match(r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$', ip) and port.isdigit() and 1 <= int(port) <= 65535:
                        proxy_lines += 1
                except:
                    continue
        
        is_proxy_list = proxy_lines / len(lines) > 0.3 if lines else False
        
        # Save to database
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO proxy_sources (url, raw_content, extracted_proxies, analyzed_at)
            VALUES (%s, %s, %s, NOW())
        """, (url, response.text[:10000], proxies))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'url': url,
            'content_length': len(response.text),
            'line_count': len(lines),
            'extracted_proxies': proxies,
            'analysis': {
                'is_proxy_list': is_proxy_list,
                'proxy_lines': proxy_lines,
                'estimated_proxy_count': len(proxies)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/admin/database-status', methods=['GET'])
def get_database_status():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get database size
    cur.execute("SELECT pg_database_size(current_database()) as db_size")
    db_size = cur.fetchone()['db_size']
    
    # Get table sizes
    cur.execute("""
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables 
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """)
    tables = cur.fetchall()
    
    # Get row counts
    cur.execute("""
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    """)
    stats = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return jsonify({
        'db_size': db_size,
        'tables': tables,
        'stats': stats,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/admin/reset-database', methods=['POST'])
def reset_database():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Backup recent data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS proxy_results_backup (
                LIKE proxy_results INCLUDING ALL
            )
        """)
        
        cur.execute("""
            INSERT INTO proxy_results_backup
            SELECT * FROM proxy_results
            ORDER BY created_at DESC
            LIMIT 1000
        """)
        
        # Delete old data
        cur.execute("""
            DELETE FROM proxy_results 
            WHERE created_at < NOW() - INTERVAL '7 days'
        """)
        
        # Delete expired cache
        cur.execute("""
            DELETE FROM geoip_cache 
            WHERE expires_at < NOW()
        """)
        
        # Vacuum
        cur.execute("VACUUM ANALYZE")
        
        conn.commit()
        
        # Log and notify
        log_system('info', 'Database reset completed', 'database')
        send_telegram_message("🗄️ *Database Reset Completed*\nOld data cleaned up successfully")
        
        return jsonify({'message': 'Database reset completed'})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/health')
def health_check():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
