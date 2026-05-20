from app.db.session import engine
from sqlalchemy import text

with engine.connect() as conn:
    res = conn.execute(text('SELECT status, COUNT(*) FROM documents GROUP BY status'))
    print('status counts:')
    for row in res:
        print(row[0], row[1])
    print('---')
    res2 = conn.execute(text('SELECT doc_id, filename, status, page_count, chunk_count FROM documents ORDER BY updated_at DESC LIMIT 20'))
    print('recent docs:')
    for row in res2:
        print(row)
