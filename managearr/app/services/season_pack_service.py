"""Manual-only strict Sonarr season-pack preview and confirmed exact grab."""
import hashlib, secrets
from ..adapters.sonarr_client import SonarrClient, SonarrError, SonarrPostAmbiguousError, SonarrPostRejectedError
from ..domain.season_pack import PROTOCOLS, SeasonInventory, evaluate_release, strict_int
from ..persistence.season_pack_repository import library_identity_digest

class SeasonPackService:
    def __init__(self,repo,libraries,live_repo,scheduler_repo,client_factory=SonarrClient,timeout=None):
        self.repo,self.libraries,self.live_repo,self.scheduler_repo=repo,libraries,live_repo,scheduler_repo
        self.client_factory,self.timeout=client_factory,timeout
    def _client(self,lib):return self.client_factory(lib.url,lib.api_key,**({"timeout":self.timeout} if self.timeout is not None else {}))
    def settings(self,library_id):
        lib=self.libraries.get(library_id)
        return self.repo.settings(library_id) if lib and lib.type=="sonarr" else None
    def update_settings(self,library_id,payload):
        lib=self.libraries.get(library_id)
        if not lib or lib.type!="sonarr":return None,["Sonarr library not found"]
        data={**self.repo.settings(library_id),**(payload if isinstance(payload,dict) else {})};errors=[]
        for k in ("enabled","allow_cutoff_override"):
            if not isinstance(data.get(k),bool):errors.append(f"{k} must be a boolean")
        if data.get("protocol") is not None and data.get("protocol") not in PROTOCOLS:errors.append("protocol must be usenet or torrent")
        if data.get("download_client_id") is not None and not strict_int(data.get("download_client_id"),positive=True):errors.append("download_client_id must be a positive integer")
        if data.get("enabled") and (data.get("protocol") not in PROTOCOLS or not strict_int(data.get("download_client_id"),positive=True)):errors.append("enabled season packs require a preferred protocol and download_client_id")
        for k,lo,hi in (("hourly_grab_cap",1,10),("cooldown_minutes",1,10080),("pacing_seconds",5,3600)):
            if not strict_int(data.get(k)) or not lo<=data[k]<=hi:errors.append(f"{k} must be between {lo} and {hi}")
        if errors:return None,errors
        return self.repo.update_settings(library_id,data),[]
    def clients(self,library_id):
        lib=self.libraries.get(library_id)
        if not lib or lib.type!="sonarr":return None,"Sonarr library not found"
        try:return self._client(lib).get_download_clients(),None
        except SonarrError as e:return None,str(e)
    def preview(self,library_id,payload):
        if not isinstance(payload,dict):return None,"request body must be an object"
        sid,snum=payload.get("series_id"),payload.get("season_number")
        if not strict_int(sid,positive=True) or not strict_int(snum,positive=True):return None,"series_id and season_number must be positive integers; specials are not supported"
        lib=self.libraries.get(library_id);settings=self.repo.settings(library_id)
        if not lib or lib.type!="sonarr" or not lib.enabled:return None,"enabled Sonarr library not found"
        if not settings["enabled"]:return None,"season-pack grabs are disabled for this library"
        if settings["protocol"] not in PROTOCOLS or not strict_int(settings["download_client_id"],positive=True):return None,"configured preferred protocol and download client are required"
        client=self._client(lib)
        try:
            clients=client.get_download_clients();match=next((x for x in clients if x["id"]==settings["download_client_id"]),None)
            if not match or not match["enabled"] or match["protocol"]!=settings["protocol"]:return None,"configured preferred download client is missing, disabled, or protocol-mismatched"
            inv,error=SeasonInventory.parse(client.get_series_detail(sid),client.get_episodes(sid),sid,snum)
            if error:return None,error
            raw=client.search_season_releases(sid,snum)
        except SonarrError as e:return None,str(e)
        accepted=[];rejected=[]
        for i,r in enumerate(raw):
            route_evidence={"id":match["id"],"enabled":match["enabled"],"protocol":match["protocol"]}
            decision,reasons=evaluate_release(r,inv,settings["protocol"],settings["allow_cutoff_override"],download_client_id=settings["download_client_id"],routing_evidence=route_evidence)
            if decision:accepted.append((decision.weight,i,decision))
            else:rejected.append({"title":r.get("title")[:200] if isinstance(r,dict) and isinstance(r.get("title"),str) else "(malformed release)","reasons":reasons[:12]})
        accepted.sort(key=lambda x:(x[0],x[1]));selected=accepted[0][2] if accepted else None
        token=secrets.token_urlsafe(32);digest=hashlib.sha256(token.encode()).hexdigest()
        snapshot={k:settings[k] for k in ("protocol","download_client_id","allow_cutoff_override","hourly_grab_cap","cooldown_minutes","pacing_seconds")}
        snapshot.update({"library_id":lib.id,"library_type":lib.type,"library_enabled":lib.enabled,"library_identity_digest":library_identity_digest(lib.to_dict()),"library_config_generation":lib.updated_at})
        audit=self.repo.create_preview({"library_id":library_id,"series_id":sid,"season_number":snum,"series_title":inv.series_title,"selected_fingerprint":selected.fingerprint if selected else None,"selected_title":selected.title if selected else None,"selected_protocol":selected.protocol if selected else None,"selected_indexer_id":selected.indexer_id if selected else None,"episode_count":len(inv.episode_ids),"cutoff_override":selected.cutoff_override if selected else False,"rejected_summary":rejected[:100],"confirmation_digest":digest,"settings_snapshot":snapshot})
        return {"audit":self.safe_audit(audit),"selected":selected.safe_dict() if selected else None,"rejected":rejected,"confirmation_token":token if selected else None,"expires_in_seconds":600},None
    def confirm(self,audit_id,payload):
        if not isinstance(payload,dict) or payload.get("confirm") is not True:return None,"confirm must be true"
        token=payload.get("confirmation_token")
        if not isinstance(token,str) or not token:return None,"confirmation_token is required"
        before=self.repo.get(audit_id)
        if before and before["state"] in ("completed","blocked","ambiguous"):return self.safe_audit(before),None
        status=self._live_status()
        if not status["allowed"]:return None,"season-pack grab blocked: "+"; ".join(status["reasons"])
        row,error=self.repo.claim(audit_id,hashlib.sha256(token.encode()).hexdigest(),status["generation"])
        if error:return None,error
        if row["state"]!="dispatching":return self.safe_audit(row),None
        current=self._live_status()
        if not current["allowed"] or current["generation"]!=row["live_generation"]:return self.safe_audit(self.repo.finalize(audit_id,"blocked","Live authorization changed before grab")),None
        lib=self.libraries.get(row["library_id"]);s=row["settings_snapshot"];attempted=False
        try:
            if not lib or lib.type!="sonarr" or not lib.enabled or library_identity_digest(lib.to_dict())!=s.get("library_identity_digest") or lib.updated_at!=s.get("library_config_generation"):
                return self.safe_audit(self.repo.finalize(audit_id,"blocked","library routing identity changed since preview")),None
            client=self._client(lib)
            current_settings=self.repo.settings(row["library_id"])
            setting_keys=("protocol","download_client_id","allow_cutoff_override","hourly_grab_cap","cooldown_minutes","pacing_seconds")
            if not current_settings.get("enabled") or any(current_settings.get(k)!=s.get(k) for k in setting_keys):
                return self.safe_audit(self.repo.finalize(audit_id,"blocked","season-pack settings changed since preview")),None
            clients=client.get_download_clients()
            route=next((x for x in clients if x["id"]==s["download_client_id"]),None)
            if not route or not route["enabled"] or route["protocol"]!=s["protocol"]:
                return self.safe_audit(self.repo.finalize(audit_id,"blocked","configured preferred download client is no longer valid")),None
            inv,e=SeasonInventory.parse(client.get_series_detail(row["series_id"]),client.get_episodes(row["series_id"]),row["series_id"],row["season_number"])
            if e:return self.safe_audit(self.repo.finalize(audit_id,"blocked",e)),None
            matches=[]
            for raw in client.search_season_releases(row["series_id"],row["season_number"]):
                route_evidence={"id":route["id"],"enabled":route["enabled"],"protocol":route["protocol"]}
                d,_=evaluate_release(raw,inv,s["protocol"],s["allow_cutoff_override"],download_client_id=s["download_client_id"],routing_evidence=route_evidence)
                if d and d.fingerprint==row["selected_fingerprint"]:matches.append(d)
            if len(matches)!=1:return self.safe_audit(self.repo.finalize(audit_id,"blocked","exact previewed release is missing or ambiguous on revalidation")),None
            d=matches[0];body={"guid":d.guid,"indexerId":d.indexer_id,"downloadClientId":s["download_client_id"]}
            if d.cutoff_override:body.update({"shouldOverride":True,"seriesId":inv.series_id,"episodeIds":list(d.episode_ids),"quality":d.quality,"languages":d.languages})
            with self.repo.authorized_write(audit_id,row["live_generation"]) as (conn,authorization_error):
                if authorization_error:return self.safe_audit(self.repo.finalize(audit_id,"blocked",authorization_error,conn=conn)),None
                try:
                    attempted=True
                    client.grab_release(body)
                except SonarrPostRejectedError as e:return self.safe_audit(self.repo.finalize(audit_id,"blocked",str(e),conn=conn)),None
                except SonarrPostAmbiguousError as e:return self.safe_audit(self.repo.finalize(audit_id,"ambiguous",str(e),conn=conn)),None
                except Exception:return self.safe_audit(self.repo.finalize(audit_id,"ambiguous","unexpected error after grab attempt",conn=conn)),None
                return self.safe_audit(self.repo.finalize(audit_id,"completed",conn=conn)),None
        except SonarrError as e:return self.safe_audit(self.repo.finalize(audit_id,"ambiguous" if attempted else "blocked",str(e))),None
        except Exception:return self.safe_audit(self.repo.finalize(audit_id,"ambiguous" if attempted else "blocked","unexpected error after grab attempt" if attempted else "unexpected error before grab attempt")),None
    def _live_status(self):
        mode=self.scheduler_repo.get_settings().mode;c=self.live_repo.get_control();reasons=[]
        if mode!="live":reasons.append("scheduler mode is not live")
        if c["state"]!="running":reasons.append("Live authorization is paused or emergency-stopped")
        return {"allowed":not reasons,"reasons":reasons,"generation":c.get("authorization_generation",0)}
    @staticmethod
    def safe_audit(row):
        if not row:return None
        keys=("id","library_id","series_id","season_number","series_title","state","selected_fingerprint","selected_title","selected_protocol","selected_indexer_id","episode_count","cutoff_override","rejected_summary","error_summary","created_at","updated_at")
        return {k:(row[k].isoformat() if hasattr(row.get(k),"isoformat") else row.get(k)) for k in keys}
