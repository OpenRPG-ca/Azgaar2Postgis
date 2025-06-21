import psycopg2
from datetime import datetime, timezone


def get_next_version(pg_url, name):
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT "version" FROM regular."FileUpload"
                WHERE "name"=%s
                ORDER BY "createdDate" DESC
                LIMIT 1
                """,
                (name,),
            )
            row = cur.fetchone()

    version_str = str(row[0]) if row and row[0] else "0.0.1"

    try:
        parts = [int(p) for p in version_str.strip().split(".")]
        while len(parts) < 3:
            parts.append(0)
        major, minor, patch = parts[:3]
        patch += 1
        next_version = f"{major}.{minor}.{patch}"
        return next_version
    except Exception as e:
        print("Error parsing version:", e)
        return "0.0.1"


def set_previous_active_to_passed(pg_url):
    """Sets the currently active file (if any) to 'passed' and returns its version (if any)."""
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            # Get the version of the currently active file (if any)
            cur.execute(
                """
                SELECT "version" FROM regular."FileUpload"
                WHERE "status" = 'active'
                ORDER BY "createdDate" DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            version = row[0] if row else None

            # Update status to 'passed'
            cur.execute(
                """
                UPDATE regular."FileUpload"
                SET "status" = 'passed'
                WHERE "status" = 'active'
                """
            )
        conn.commit()
    return version


def insert_fileupload_entry(name, baseName, path, version, status, pg_url):
    created_date = datetime.now(timezone.utc)
    print(
        f"Inserting fileupload entry with variables:"
        f"\n  name: {name}"
        f"\n  baseName: {baseName}"
        f"\n  path: {path}"
        f"\n  version: {version}"
        f"\n  status: {status}"
        f"\n  created_date: {created_date}"
    )
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO regular."FileUpload" ("name", "baseName", "path", "version", "status", "createdDate")
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ("name") DO NOTHING
                """,
                (name, baseName, path, version, status, created_date),
            )
        conn.commit()


def set_active_status(pg_url, name):
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE regular."FileUpload"
                SET "status"=%s
                WHERE "name"=%s AND "status"=%s
                """,
                ("active", name, "passed"),
            )
        conn.commit()


def update_fileupload_status(path, baseName, name, status, version, pg_url):
    print(f"Next version for {name} is {version}")
    created_date = datetime.now(timezone.utc)

    print(f"Updating fileupload status for {name} to {status} with version {version}")
    if status == "active":
        set_active_status(pg_url, name)

    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO regular."FileUpload" ("name", "baseName", "path", "version", "status", "createdDate")
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ("name")
                DO UPDATE SET "version" = EXCLUDED."version",
                            "status" = EXCLUDED."status",
                            "createdDate" = EXCLUDED."createdDate";
                """,
                (name, baseName, path, version, status, created_date),
            )
        conn.commit()
    return version
