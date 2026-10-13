"""Initialize local configuration without overwriting existing files."""
import argparse
import base64
import os
from pathlib import Path
import re
import secrets
import shutil

ROOT = Path(__file__).resolve().parents[1]


def write_new(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('x', encoding='utf-8') as file:
            file.write(content)
    except FileExistsError:
        print(f'KEEP: {path.relative_to(ROOT)}')
    else:
        print(f'CREATE: {path.relative_to(ROOT)}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-name', help='Unique storage name, for example layer-test-v3')
    args = parser.parse_args()
    if args.test_name and not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}', args.test_name):
        parser.error('--test-name must contain lowercase letters, digits, and hyphens')

    templates = [
        'infra/airflow/.env.example',
        'infra/dbt/profiles.example.yml',
        'dbt/profiles.example.yml',
        'infra/trino/catalog/iceberg.properties.example',
    ]
    for name in templates:
        if not (ROOT / name).is_file():
            raise SystemExit(f'ERROR: Missing template: {name}')

    env_path = ROOT / 'infra/airflow/.env'
    if not env_path.exists():
        values = {
            'MINIO_ACCESS_KEY': 'lakehouse',
            'MINIO_SECRET_KEY': secrets.token_hex(24),
            'FERNET_KEY': base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
            'AIRFLOW__API_AUTH__JWT_SECRET': secrets.token_hex(32),
            'AIRFLOW_UID': str(os.getuid() if os.uname().sysname == 'Linux' else 50000),
            '_AIRFLOW_WWW_USER_USERNAME': 'airflow',
            '_AIRFLOW_WWW_USER_PASSWORD': secrets.token_hex(16),
        }
        content = (ROOT / templates[0]).read_text()
        for key, value in values.items():
            pattern = rf'(?m)^{re.escape(key)}=.*$'
            if re.search(pattern, content):
                content = re.sub(pattern, f'{key}={value}', content)
            else:
                content += f'\n{key}={value}\n'
        write_new(env_path, content.rstrip() + '\n')
        env_path.chmod(0o600)
    else:
        print('KEEP: infra/airflow/.env')

    # Read simple KEY=value entries without executing the environment file.
    values = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip().strip('"\'')

    required = ['MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'FERNET_KEY',
                'AIRFLOW__API_AUTH__JWT_SECRET', '_AIRFLOW_WWW_USER_PASSWORD']
    missing = [key for key in required if not values.get(key)
               or 'REPLACE_WITH' in values[key] or 'YOUR_' in values[key]]
    if missing:
        raise SystemExit('ERROR: Complete existing .env values first: ' + ', '.join(missing))
    if values['MINIO_ACCESS_KEY'] != 'lakehouse':
        raise SystemExit('ERROR: Current Compose requires MINIO_ACCESS_KEY=lakehouse')

    for template, destination in [
        ('infra/dbt/profiles.example.yml', 'infra/dbt/profiles.yml'),
        ('dbt/profiles.example.yml', 'dbt/profiles.yml'),
    ]:
        write_new(ROOT / destination, (ROOT / template).read_text())

    iceberg_path = ROOT / 'infra/trino/catalog/iceberg.properties'
    if iceberg_path.exists():
        settings = {}
        for line in iceberg_path.read_text().splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                settings[key.strip()] = value.strip()
        if (settings.get('s3.aws-access-key') != values['MINIO_ACCESS_KEY']
                or settings.get('s3.aws-secret-key') != values['MINIO_SECRET_KEY']):
            raise SystemExit('ERROR: Existing Iceberg credentials do not match .env; reconcile manually')
        print('KEEP: infra/trino/catalog/iceberg.properties')
    else:
        content = (ROOT / templates[3]).read_text()
        content = re.sub(r'(?m)^s3.aws-access-key=.*$',
                         lambda _: 's3.aws-access-key=' + values['MINIO_ACCESS_KEY'], content)
        content = re.sub(r'(?m)^s3.aws-secret-key=.*$',
                         lambda _: 's3.aws-secret-key=' + values['MINIO_SECRET_KEY'], content)
        write_new(iceberg_path, content)
        iceberg_path.chmod(0o600)

    for directory in ['infra/airflow/config', 'logs/airflow', 'plugins', 'data/instacart', 'data/batches']:
        (ROOT / directory).mkdir(parents=True, exist_ok=True)

    if args.test_name:
        prefix = 'instacart-' + args.test_name
        write_new(ROOT / 'infra/generated-test-volumes.yaml',
                  f'volumes:\n  minio_data:\n    name: {prefix}-minio\n'
                  f'  nessie_data:\n    name: {prefix}-nessie\n')
        write_new(ROOT / 'infra/airflow/generated-test-volumes.yaml',
                  f'volumes:\n  postgres-db-volume:\n    name: {prefix}-airflow\n')
        print('Check that generated volume names are unused before a fresh-storage test.')
        print('Existing override files are preserved; verify their names if choosing a new test name.')

    print('PASS: Local configuration initialized. No services started.')
    print('Airflow username/password are stored in infra/airflow/.env; keep this file private.')


if __name__ == '__main__':
    main()
