from flask import Flask, render_template, request, redirect, url_for, session, flash
import os

app = Flask(__name__)
# In a real production app, use a random secure string.
app.secret_key = 'pdm_super_secret_key_123'

# Hardcoded credentials for demonstration
VALID_USERNAME = 'admin'
VALID_PASSWORD = 'password123'

@app.route('/')
def dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    return render_template('dashboard.html', username=session.get('username'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username == VALID_USERNAME and password == VALID_PASSWORD:
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password')
            return render_template('login.html')
            
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

if __name__ == '__main__':
    # Run on all interfaces so it's accessible externally if needed
    app.run(host='0.0.0.0', port=5000, debug=True)
