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
    assert "Simulation never searches releases or grabs" in html
    assert 'id="sp-confirm-check"' in html and 'confirm:true' in html
    assert "Emergency Stop block confirmation" in html


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
