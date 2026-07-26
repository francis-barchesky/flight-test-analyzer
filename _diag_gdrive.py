import json, pathlib, re, urllib.parse, urllib.request

cfg = json.loads((pathlib.Path.home() / '.cia_config.json').read_text(encoding='utf-8-sig'))
gtoken = cfg.get('googleToken', '')
jtoken = cfg.get('jiraToken', '')
jauth  = cfg.get('jiraAuthType', 'Basic')

def gget(label, url):
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {gtoken}', 'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read())
            print(f'[{label}] 200: {str(d)[:300]}')
            return d
    except urllib.error.HTTPError as e:
        print(f'[{label}] {e.code}: {e.read().decode("utf-8",errors="replace")[:200]}')
    except Exception as e:
        print(f'[{label}] ERR: {e}')
    return None

def jget(label, url):
    req = urllib.request.Request(url, headers={'Authorization': f'{jauth} {jtoken}', 'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f'[{label}] ERR: {e}')
    return None

# 1. Fetch the Jira issue for S140N208B and look at ALL fields
print('=== Jira issue fields for S140N208B ===')
jql = 'project = FFT AND issuetype = "Flight Test" AND summary ~ "S140N208B" ORDER BY updated DESC'
url = f'https://merlinlabs.atlassian.net/rest/api/3/search/jql?jql={urllib.parse.quote(jql)}&maxResults=1'
data = jget('jira search', url)
if data:
    print('top-level keys:', list(data.keys()))
    issues = data.get('issues', data.get('items', []))
    if issues:
        issue = issues[0]
        print('issue keys:', list(issue.keys()))
        key = issue.get('key') or issue.get('id', '?')
        print(f'key={key}')
        fields = issue.get('fields', {})
        for k, v in fields.items():
            if v is not None and v != '' and v != []:
                print(f'  {k}: {str(v)[:120]}')
    else:
        print('raw:', str(data)[:500])

# 2. Try broader GDrive searches
print('\n=== GDrive searches ===')
for q in [
    "name contains 'S140' and mimeType = 'application/vnd.google-apps.document'",
    "name contains 'N208B' and mimeType = 'application/vnd.google-apps.document'",
    "name contains 'flight card' and mimeType = 'application/vnd.google-apps.document'",
    "name contains 'Flight Card' and mimeType = 'application/vnd.google-apps.document'",
    "name contains 'S140'",
]:
    url = ('https://www.googleapis.com/drive/v3/files'
           f'?q={urllib.parse.quote(q)}'
           '&fields=files(id,name)&includeItemsFromAllDrives=true&supportsAllDrives=true&pageSize=5')
    d = gget(q[:50], url)
    if d and d.get('files'):
        for f in d['files']:
            print(f'  -> {f["name"]}  ({f["id"]})')
