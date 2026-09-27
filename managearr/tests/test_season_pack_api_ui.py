class StubSeasonPack:
    def settings(self,library_id):return {"library_id":library_id,"enabled":False}
    def update_settings(self,library_id,payload):return {"library_id":library_id,**payload},[]
    def clients(self,library_id):return [{"id":9,"name":"NZB","protocol":"usenet","enabled":True}],None
    def preview(self,library_id,payload):return {"audit":{"id":4},"selected":{"fingerprint":"a"*64},"rejected":[],"confirmation_token":"once","expires_in_seconds":600},None
    def confirm(self,audit_id,payload):return {"id":audit_id,"state":"completed"},None

def test_season_pack_api_preview_and_explicit_confirm(client,app,monkeypatch):
    monkeypatch.setitem(app.extensions["managearr"],"season_pack",StubSeasonPack())
    response=client.post("/api/v1/libraries/1/season-packs/preview",json={"series_id":7,"season_number":2})
    assert response.status_code==201 and response.get_json()["confirmation_token"]=="once"
    response=client.post("/api/v1/season-packs/4/confirm",json={"confirm":True,"confirmation_token":"once"})
    assert response.status_code==200 and response.get_json()["audit"]["state"]=="completed"

def test_season_pack_ui_states_manual_safety_boundary(client):
    html=client.get("/season-packs").get_data(as_text=True)
    assert "Manual only" in html and "never sends broad SeasonSearch" in html
    assert "manual preview performs read-only interactive release GETs" in html and "automatic scheduler runs never search releases or grab" in html
    assert 'id="sp-confirm-check"' in html and 'confirm:true' in html
    assert "Emergency Stop block confirmation" in html


import copy
import threading
import time
import pytest

from app.adapters.sonarr_client import SonarrPostAmbiguousError, SonarrPostRejectedError
from app.persistence.season_pack_repository import SeasonPackRepository
from app.services.season_pack_service import SeasonPackService

class ExactClient:
    grabs=[]
    def __init__(self,*args,**kwargs):pass
    def get_download_clients(self):return [{"id":9,"name":"NZB","protocol":"usenet","enabled":True}]
    def get_series_detail(self,series_id):return {"id":series_id,"title":"Show","seriesType":"standard"}
    def get_episodes(self,series_id):return [{"id":101,"seasonNumber":2,"episodeNumber":1},{"id":102,"seasonNumber":2,"episodeNumber":2}]
    def search_season_releases(self,series_id,season_number):
        return [{"guid":"private-guid","indexerId":5,"title":"Show.S02.1080p-GRP","fullSeason":True,"mappedSeriesId":series_id,"mappedSeasonNumber":season_number,"mappedEpisodeInfo":[{"id":101,"seasonNumber":2,"episodeNumber":1},{"id":102,"seasonNumber":2,"episodeNumber":2}],"quality":{"quality":{"id":4,"name":"HDTV-720p"},"revision":{"version":1,"real":0,"isRepack":False}},"languages":[{"id":1,"name":"English"}],"approved":True,"downloadAllowed":True,"rejected":False,"temporarilyRejected":False,"rejections":[],"releaseWeight":1,"protocol":"usenet"}]
    def grab_release(self,body):self.grabs.append(body);return {}

def test_real_service_exact_grab_is_live_gated_and_idempotent(client,app,database,library_repo,monkeypatch):
    library=library_repo.create({"name":"Sonarr","type":"sonarr","url":"http://sonarr","api_key":"secret","enabled":True})
    service=app.extensions["managearr"]["season_pack"];monkeypatch.setattr(service,"client_factory",ExactClient);ExactClient.grabs=[]
    settings={"enabled":True,"protocol":"usenet","download_client_id":9,"allow_cutoff_override":False,"hourly_grab_cap":1,"cooldown_minutes":1440,"pacing_seconds":30}
    assert client.patch(f"/api/v1/libraries/{library.id}/season-packs/settings",json=settings).status_code==200
    preview=client.post(f"/api/v1/libraries/{library.id}/season-packs/preview",json={"series_id":7,"season_number":2}).get_json()
    assert preview["selected"] and "private-guid" not in str(preview)
    blocked=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]})
    assert blocked.status_code==422 and ExactClient.grabs==[]
    with database.connect() as conn:conn.execute("UPDATE scheduler_settings SET mode='live',updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running",actor="test",reason="M11 test")
    first=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]})
    second=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]})
    assert first.get_json()["audit"]["state"]=="completed" and second.get_json()["audit"]["state"]=="completed"
    assert ExactClient.grabs==[{"guid":"private-guid","indexerId":5,"downloadClientId":9}]

class ChangingClient(ExactClient):
    calls=0;change=None;hook=None;grab_error=None
    def search_season_releases(self,series_id,season_number):
        result=super().search_season_releases(series_id,season_number)
        type(self).calls+=1
        if type(self).calls==2:
            if type(self).hook:type(self).hook()
            if type(self).change:result[0].update(copy.deepcopy(type(self).change))
        return result
    def grab_release(self,body):
        if type(self).grab_error:raise type(self).grab_error("test outcome")
        return super().grab_release(body)

def prepare(client,app,database,library_repo,monkeypatch):
    library=library_repo.create({"name":"Sonarr","type":"sonarr","url":"http://sonarr","api_key":"secret","enabled":True})
    service=app.extensions["managearr"]["season_pack"];monkeypatch.setattr(service,"client_factory",ChangingClient)
    ChangingClient.grabs=[];ChangingClient.calls=0;ChangingClient.change=None;ChangingClient.hook=None;ChangingClient.grab_error=None
    settings={"enabled":True,"protocol":"usenet","download_client_id":9,"allow_cutoff_override":True,"hourly_grab_cap":1,"cooldown_minutes":1440,"pacing_seconds":30}
    assert client.patch(f"/api/v1/libraries/{library.id}/season-packs/settings",json=settings).status_code==200
    preview=client.post(f"/api/v1/libraries/{library.id}/season-packs/preview",json={"series_id":7,"season_number":2}).get_json()
    with database.connect() as conn:conn.execute("UPDATE scheduler_settings SET mode='live',updated_at=now() WHERE id=1")
    app.extensions["managearr"]["live_repo"].set_authorization_state("running",actor="test",reason="M11 test")
    return library,preview

@pytest.mark.parametrize("change",[
    {"approved":False,"rejected":True,"rejections":["Existing file meets cutoff: HDTV-720p"]},
    {"quality":{"quality":{"id":5,"name":"WEBDL-720p"},"revision":{"version":1,"real":0,"isRepack":False}}},
    {"languages":[{"id":2,"name":"French"}]},
    {"temporarilyRejected":True,"approved":False,"rejected":True,"rejections":["Temporary rejection"]},
])
def test_changed_release_decision_requires_new_preview(client,app,database,library_repo,monkeypatch,change):
    _,preview=prepare(client,app,database,library_repo,monkeypatch);ChangingClient.change=change
    result=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]}).get_json()["audit"]
    assert result["state"]=="blocked" and ChangingClient.grabs==[]

@pytest.mark.parametrize("change",["disabled","type","url","api_key"])
def test_library_routing_identity_change_blocks(client,app,database,library_repo,monkeypatch,change):
    library,preview=prepare(client,app,database,library_repo,monkeypatch)
    payload={"enabled":False} if change=="disabled" else ({"type":"radarr"} if change=="type" else ({"url":"http://other"} if change=="url" else {"api_key":"other-secret"}))
    library_repo.update(library.id,payload)
    result=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]}).get_json()["audit"]
    assert result["state"]=="blocked" and ChangingClient.grabs==[] and "secret" not in str(result)

def test_live_change_during_release_revalidation_blocks_final_write(client,app,database,library_repo,monkeypatch):
    _,preview=prepare(client,app,database,library_repo,monkeypatch)
    ChangingClient.hook=lambda:app.extensions["managearr"]["live_repo"].set_authorization_state("paused",actor="test",reason="during revalidation")
    result=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]}).get_json()["audit"]
    assert result["state"]=="blocked" and ChangingClient.grabs==[]

@pytest.mark.parametrize("error,state",[(SonarrPostAmbiguousError,"ambiguous"),(SonarrPostRejectedError,"blocked")])
def test_typed_post_outcome_persists_safe_terminal_state(client,app,database,library_repo,monkeypatch,error,state):
    _,preview=prepare(client,app,database,library_repo,monkeypatch);ChangingClient.grab_error=error
    result=client.post(f"/api/v1/season-packs/{preview['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":preview["confirmation_token"]}).get_json()["audit"]
    assert result["state"]==state
    if state=="ambiguous":
        newer=client.post(f"/api/v1/libraries/{result['library_id']}/season-packs/preview",json={"series_id":7,"season_number":2}).get_json()
        blocked=client.post(f"/api/v1/season-packs/{newer['audit']['id']}/confirm",json={"confirm":True,"confirmation_token":newer["confirmation_token"]})
        assert blocked.status_code==422 and "cap" in blocked.get_json()["errors"][0]


class SimulatedProcessDeath(BaseException):
    pass


class CrashAfterAcceptedClient(ExactClient):
    def grab_release(self,body):
        super().grab_release(body)
        raise SimulatedProcessDeath("process died after Sonarr accepted the POST")


def test_crash_after_accepted_post_is_durably_ambiguous_and_never_resent(client,app,database,library_repo,monkeypatch):
    _,preview=prepare(client,app,database,library_repo,monkeypatch)
    service=app.extensions["managearr"]["season_pack"]
    monkeypatch.setattr(service,"client_factory",CrashAfterAcceptedClient)
    ExactClient.grabs=[]

    with pytest.raises(SimulatedProcessDeath):
        service.confirm(preview["audit"]["id"],{"confirm":True,"confirmation_token":preview["confirmation_token"]})

    with database.connect() as conn:
        stored=conn.execute("SELECT state FROM season_pack_audit WHERE id=%s",(preview["audit"]["id"],)).fetchone()
        marker=conn.execute("SELECT started_at FROM season_pack_attempt_started WHERE audit_id=%s",(preview["audit"]["id"],)).fetchone()
    assert stored["state"]=="dispatching" and marker["started_at"] is not None

    # A new repository/service instance models restart: the committed marker,
    # not process memory or a stale timeout, must make the result ambiguous.
    restarted=SeasonPackService(SeasonPackRepository(database),library_repo,app.extensions["managearr"]["live_repo"],app.extensions["managearr"]["scheduler_repo"],client_factory=CrashAfterAcceptedClient)
    result,error=restarted.confirm(preview["audit"]["id"],{"confirm":True,"confirmation_token":preview["confirmation_token"]})
    assert error is None and result["state"]=="ambiguous"
    assert "no terminal result" in result["error_summary"]
    assert len(ExactClient.grabs)==1

    with pytest.raises(Exception,match="append-only"):
        with database.connect() as conn:
            conn.execute("UPDATE season_pack_attempt_started SET started_at=now() WHERE audit_id=%s",(preview["audit"]["id"],))
    with pytest.raises(Exception,match="append-only"):
        with database.connect() as conn:
            conn.execute("DELETE FROM season_pack_attempt_started WHERE audit_id=%s",(preview["audit"]["id"],))


class BlockingAcceptedClient(ExactClient):
    entered=threading.Event();release=threading.Event()
    def grab_release(self,body):
        type(self).entered.set()
        assert type(self).release.wait(3)
        return super().grab_release(body)


def test_pause_waits_for_attempt_boundary_and_bounded_post(client,app,database,library_repo,monkeypatch):
    _,preview=prepare(client,app,database,library_repo,monkeypatch)
    service=app.extensions["managearr"]["season_pack"]
    monkeypatch.setattr(service,"client_factory",BlockingAcceptedClient)
    BlockingAcceptedClient.entered=threading.Event();BlockingAcceptedClient.release=threading.Event();ExactClient.grabs=[]
    outcome={};pause_done=threading.Event()

    def confirm():
        outcome["confirm"]=service.confirm(preview["audit"]["id"],{"confirm":True,"confirmation_token":preview["confirmation_token"]})
    def pause():
        app.extensions["managearr"]["live_repo"].set_authorization_state("paused",actor="test",reason="race")
        pause_done.set()

    confirm_thread=threading.Thread(target=confirm);confirm_thread.start()
    assert BlockingAcceptedClient.entered.wait(3)
    pause_thread=threading.Thread(target=pause);pause_thread.start()
    time.sleep(.1)
    assert not pause_done.is_set()
    BlockingAcceptedClient.release.set()
    confirm_thread.join(3);pause_thread.join(3)
    assert not confirm_thread.is_alive() and not pause_thread.is_alive()
    assert outcome["confirm"][0]["state"]=="completed"
    assert pause_done.is_set() and len(ExactClient.grabs)==1
