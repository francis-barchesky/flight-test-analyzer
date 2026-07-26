import json, pathlib, urllib.parse, urllib.request

cfg = json.loads((pathlib.Path.home() / '.cia_config.json').read_text(encoding='utf-8-sig'))
token = cfg.get('jiraToken', '')
auth_type = cfg.get('jiraAuthType', 'Basic')
base = 'https://merlinlabs.atlassian.net'

def get(label, url):
    req = urllib.request.Request(url, headers={
        'Authorization': f'{auth_type} {token}',
        'Accept': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read())
            issues = d.get('issues', [])
            print(f'[{label}] 200: {len(issues)} issues')
            for i in issues[:5]:
                print(f'  key={i["key"]}  summary={i["fields"]["summary"]}')
    except urllib.error.HTTPError as e:
        print(f'[{label}] {e.code}: {e.read().decode("utf-8", errors="replace")[:300]}')
    except Exception as e:
        print(f'[{label}] ERR: {e}')

jql_s140 = 'project = FFT AND issuetype = "Flight Test" AND summary ~ "S140N208B" ORDER BY updated DESC'
get('/rest/api/3/search/jql', f'{base}/rest/api/3/search/jql?jql={urllib.parse.quote(jql_s140)}&maxResults=3&fields=summary,customfield_10042')
