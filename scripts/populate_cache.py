import csv
import json
from pathlib import Path


def populate_cache():
    input_file = Path('data/08-github-classify/all.backup.csv')
    cache_base = Path('.cache/github-classify/qwen2.5/7b')

    if not input_file.exists():
        print(f"Error: {input_file} not found.")
        return

    count = 0
    with open(input_file, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            owner = row['owner']
            repo = row['repo']
            # Reconstruct RepoClassification structure
            classification = {
                'category': row['category'],
                'primary_framework': row.get('framework') or None,
                'framework_version': row.get('framework_version') or None,
                'description': {
                    'en': row['description_en'],
                    'zh': row['description_zh'],
                },
                'tags': [t.strip() for t in row['tags'].split(',')] if row['tags'] else [],
                'reasoning': row['reasoning'],
            }

            # Directory: .../qwen2.5/7b/owner/repo/index.json
            repo_cache_dir = cache_base / owner / repo
            repo_cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = repo_cache_dir / 'index.json'

            with open(cache_file, 'w', encoding='utf-8') as cf:
                json.dump(classification, cf, ensure_ascii=False, indent=2)
            count += 1

    print(f"Successfully populated {count} cache files to {cache_base}")


if __name__ == '__main__':
    populate_cache()
