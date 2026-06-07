import os

# Enable CORS (Embedded Superset feature flag disabled to allow legacy standalone=1 cookie login)
FEATURE_FLAGS = {"EMBEDDED_SUPERSET": False}
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Disable Talisman to allow iframes
TALISMAN_ENABLED = False

# Disable CSRF token requirement for anonymous API access (if needed)
WTF_CSRF_ENABLED = True

# Allow public role to act as Gamma (view access)
PUBLIC_ROLE_LIKE = "Gamma"

# Allow cross-origin cookies for embedding
SESSION_COOKIE_SAMESITE = "None"
SESSION_COOKIE_SECURE = True

# Set X-Frame-Options to ALLOWALL so it can be iframed anywhere
HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}

# Fix "TypeError: Cannot read properties of undefined (reading 'toString')" on login page
BABEL_DEFAULT_LOCALE = "en"
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
}
