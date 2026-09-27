"""M11 settings and append-only manual season-pack audit."""
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import hashlib, json

def library_identity_digest(row):
    if row is None:return None
    raw=json.dumps({"id":row["id"],"type":row["type"],"enabled":row["enabled"],"url":row["url"],"api_key":row["api_key"]},sort_keys=True,separators=(",",":"))
    return hashlib.sha256(raw.encode()).hexdigest()

class SeasonPackRepository:
    LOCK_CLASS=87235
    def __init__(self,db):self.db=db
    def settings(self,library_id):
        with self.db.connect() as c:row=c.execute("SELECT * FROM season_pack_settings WHERE library_id=%s",(library_id,)).fetchone()
        if not row:return {"library_id":library_id,"enabled":False,"protocol":None,"download_client_id":None,"allow_cutoff_override":False,"hourly_grab_cap":1,"cooldown_minutes":1440,"pacing_seconds":30}
        return dict(row)
    def update_settings(self,library_id,d):
        with self.db.connect() as c:c.execute("""INSERT INTO season_pack_settings(library_id,enabled,protocol,download_client_id,allow_cutoff_override,hourly_grab_cap,cooldown_minutes,pacing_seconds,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,now()) ON CONFLICT(library_id) DO UPDATE SET enabled=EXCLUDED.enabled,protocol=EXCLUDED.protocol,download_client_id=EXCLUDED.download_client_id,allow_cutoff_override=EXCLUDED.allow_cutoff_override,hourly_grab_cap=EXCLUDED.hourly_grab_cap,cooldown_minutes=EXCLUDED.cooldown_minutes,pacing_seconds=EXCLUDED.pacing_seconds,updated_at=now()""",(library_id,d["enabled"],d["protocol"],d["download_client_id"],d["allow_cutoff_override"],d["hourly_grab_cap"],d["cooldown_minutes"],d["pacing_seconds"]))
        return self.settings(library_id)
    def create_preview(self,d):
        with self.db.connect() as c:row=c.execute("""INSERT INTO season_pack_audit(library_id,series_id,season_number,series_title,state,selected_fingerprint,selected_title,selected_protocol,selected_indexer_id,episode_count,cutoff_override,rejected_summary,confirmation_digest,settings_snapshot,created_at,updated_at) VALUES(%s,%s,%s,%s,'previewed',%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,now(),now()) RETURNING id""",(d["library_id"],d["series_id"],d["season_number"],d["series_title"],d.get("selected_fingerprint"),d.get("selected_title"),d.get("selected_protocol"),d.get("selected_indexer_id"),d.get("episode_count",0),d.get("cutoff_override",False),json.dumps(d["rejected_summary"]),d["confirmation_digest"],json.dumps(d["settings_snapshot"]))).fetchone()
        return self.get(row["id"])
    def get(self,audit_id,conn=None):
        if conn:row=conn.execute("SELECT * FROM season_pack_audit WHERE id=%s",(audit_id,)).fetchone()
        else:
            with self.db.connect() as c:row=c.execute("SELECT * FROM season_pack_audit WHERE id=%s",(audit_id,)).fetchone()
        return dict(row) if row else None
    def claim(self,audit_id,digest,live_generation):
        now=datetime.now(timezone.utc)
        with self.db.connect() as c:
            row=c.execute("SELECT * FROM season_pack_audit WHERE id=%s FOR UPDATE",(audit_id,)).fetchone()
            if not row:return None,"season-pack preview not found"
            row=dict(row)
            if row["state"]=="dispatching" and row["dispatch_started_at"] and now-row["dispatch_started_at"]>timedelta(minutes=5):
                row=dict(c.execute("UPDATE season_pack_audit SET state='ambiguous',error_summary='grab reservation expired; inspect Sonarr before any retry',updated_at=now() WHERE id=%s RETURNING *",(audit_id,)).fetchone())
            if row["state"]!="previewed":return row,None
            if row["library_id"] is None:return None,"preview library no longer exists"
            c.execute("SELECT pg_advisory_xact_lock(%s,%s)",(self.LOCK_CLASS,row["library_id"]))
            if row["confirmation_digest"]!=digest:return None,"confirmation token is invalid"
            if now-row["created_at"]>timedelta(minutes=10):return None,"preview expired; create a new preview"
            s=row["settings_snapshot"]
            used=c.execute("SELECT count(*) c FROM season_pack_audit WHERE library_id=%s AND state IN ('dispatching','completed','ambiguous') AND dispatch_started_at >= %s",(row["library_id"],now-timedelta(hours=1))).fetchone()["c"]
            if used>=s["hourly_grab_cap"]:return None,"season-pack hourly grab cap reached"
            prior=c.execute("SELECT max(dispatch_started_at) at FROM season_pack_audit WHERE library_id=%s AND state IN ('dispatching','completed','ambiguous')",(row["library_id"],)).fetchone()["at"]
            if prior and (now-prior).total_seconds()<s["pacing_seconds"]:return None,"season-pack pacing interval has not elapsed"
            cool=c.execute("SELECT 1 FROM season_pack_audit WHERE library_id=%s AND series_id=%s AND season_number=%s AND state IN ('completed','ambiguous') AND dispatch_started_at >= %s LIMIT 1",(row["library_id"],row["series_id"],row["season_number"],now-timedelta(minutes=s["cooldown_minutes"]))).fetchone()
            if cool:return None,"season is in cooldown or has an ambiguous prior grab"
            c.execute("UPDATE season_pack_audit SET state='dispatching',live_generation=%s,dispatch_started_at=%s,updated_at=%s WHERE id=%s",(live_generation,now,now,audit_id))
        return self.get(audit_id),None
    @contextmanager
    def authorized_write(self,audit_id,expected_generation):
        """Linearize the last authorization check with the release POST.

        Shared row locks prevent mode, Live authorization, settings, or library
        routing changes from committing between this check and the write. The
        transaction is deliberately held only around this single bounded POST.
        """
        with self.db.connect() as c:
            row=c.execute("SELECT * FROM season_pack_audit WHERE id=%s FOR UPDATE",(audit_id,)).fetchone()
            if not row or row["state"]!="dispatching":yield c,"season-pack reservation is no longer dispatching";return
            row=dict(row);c.execute("SELECT pg_advisory_xact_lock(%s,%s)",(self.LOCK_CLASS,row["library_id"]))
            scheduler=c.execute("SELECT mode FROM scheduler_settings WHERE id=1 FOR SHARE").fetchone()
            live=c.execute("SELECT authorization_state,authorization_generation FROM live_control WHERE id=1 FOR SHARE").fetchone()
            library=c.execute("SELECT * FROM arr_libraries WHERE id=%s FOR SHARE",(row["library_id"],)).fetchone()
            settings=c.execute("SELECT * FROM season_pack_settings WHERE library_id=%s FOR SHARE",(row["library_id"],)).fetchone()
            snapshot=row["settings_snapshot"]
            error=None
            if scheduler["mode"]!="live" or live["authorization_state"]!="running" or live["authorization_generation"]!=expected_generation:
                error="Live authorization changed before grab"
            elif not library or library["type"]!="sonarr" or library["enabled"] is not True or library_identity_digest(library)!=snapshot.get("library_identity_digest") or library["updated_at"].isoformat()!=snapshot.get("library_config_generation"):
                error="library routing identity changed since preview"
            elif not settings or not settings["enabled"] or any(settings[k]!=snapshot.get(k) for k in ("protocol","download_client_id","allow_cutoff_override","hourly_grab_cap","cooldown_minutes","pacing_seconds")):
                error="season-pack settings changed since preview"
            yield c,error
    def finalize(self,audit_id,state,error="",conn=None):
        if conn is not None:
            conn.execute("UPDATE season_pack_audit SET state=%s,error_summary=%s,updated_at=now() WHERE id=%s AND state='dispatching'",(state,error[:500],audit_id));return self.get(audit_id,conn=conn)
        with self.db.connect() as c:c.execute("UPDATE season_pack_audit SET state=%s,error_summary=%s,updated_at=now() WHERE id=%s AND state='dispatching'",(state,error[:500],audit_id))
        return self.get(audit_id)
    def recent(self,limit=50):
        with self.db.connect() as c:rows=c.execute("SELECT * FROM season_pack_audit ORDER BY id DESC LIMIT %s",(limit,)).fetchall()
        return [dict(x) for x in rows]
