import copy
import pytest

from app.domain.season_pack import SeasonInventory, evaluate_release
from app.adapters.sonarr_client import SonarrClient, SonarrResponseError


def inventory():
    value,error=SeasonInventory.parse({"id":7,"title":"Show","seriesType":"standard"},[{"id":101,"seasonNumber":2,"episodeNumber":1},{"id":102,"seasonNumber":2,"episodeNumber":2}],7,2)
    assert error is None
    return value

def release(**changes):
    value={"guid":"g","indexerId":5,"title":"Show.S02.1080p-GRP","fullSeason":True,"mappedSeriesId":7,"mappedSeasonNumber":2,
           "mappedEpisodeInfo":[{"id":101,"seasonNumber":2,"episodeNumber":1},{"id":102,"seasonNumber":2,"episodeNumber":2}],
           "quality":{"quality":{"id":4,"name":"HDTV-720p"},"revision":{"version":1,"real":0,"isRepack":False}},
           "languages":[{"id":1,"name":"English"}],"approved":True,"downloadAllowed":True,"rejected":False,"temporarilyRejected":False,"rejections":[],"releaseWeight":1,"protocol":"usenet"}
    value.update(changes);return value

def test_inventory_rejects_special_daily_anime_duplicate_and_malformed():
    for season in (0,-1,True): assert SeasonInventory.parse({"id":7},[],7,season)[0] is None
    for kind in ("daily","anime",None): assert SeasonInventory.parse({"id":7,"title":"X","seriesType":kind},[],7,2)[0] is None
    dup=[{"id":1,"seasonNumber":2,"episodeNumber":1},{"id":2,"seasonNumber":2,"episodeNumber":1}]
    assert "ambiguous" in SeasonInventory.parse({"id":7,"title":"X","seriesType":"standard"},dup,7,2)[1]

@pytest.mark.parametrize("change,reason",[
    ({"fullSeason":False},"full-season"),({"mappedSeriesId":8},"mapped series"),({"mappedSeasonNumber":3},"mapped season"),
    ({"mappedEpisodeInfo":[{"id":101,"seasonNumber":2,"episodeNumber":1}]},"complete"),
    ({"mappedEpisodeInfo":[{"id":101,"seasonNumber":2,"episodeNumber":1},{"id":102,"seasonNumber":3,"episodeNumber":2}]},"cross-season"),
    ({"title":"Show.S02E01-E02"},"ambiguous"),({"title":"Show.S02.S03"},"exactly"),({"title":"Show.2025.02.03"},"exactly"),
    ({"protocol":"torrent"},"protocol"),({"downloadAllowed":False},"allow"),({"guid":""},"guid"),({"indexerId":True},"indexer"),
    ({"quality":None},"quality"),({"languages":["English"]},"language"),
])
def test_release_boundaries_fail_closed(change,reason):
    decision,reasons=evaluate_release(release(**change),inventory(),"usenet",False)
    assert decision is None and any(reason in x for x in reasons)

def test_exact_complete_release_is_selected_and_fingerprinted_without_guid_exposure():
    decision,reasons=evaluate_release(release(),inventory(),"usenet",False)
    assert reasons==[] and decision.episode_ids==(101,102)
    safe=decision.safe_dict();assert len(safe["fingerprint"])==64 and "guid" not in safe

def test_cutoff_override_is_exact_and_all_other_rejections_surface():
    cutoff=release(approved=False,rejected=True,rejections=["Existing file meets cutoff: HDTV-720p"])
    assert evaluate_release(cutoff,inventory(),"usenet",False)[0] is None
    decision,_=evaluate_release(cutoff,inventory(),"usenet",True);assert decision.cutoff_override
    for reasons in (["Existing file meets cutoff: HDTV-720p","Not enough seeders"],["Existing file meets cutoff: "],[{"message":"Existing file meets cutoff: X"}],[]):
        assert evaluate_release(release(approved=False,rejected=True,rejections=reasons),inventory(),"usenet",True)[0] is None
    _,reasons=evaluate_release(release(approved=False,rejected=True,rejections=["Custom format score too low"]),inventory(),"usenet",True)
    assert reasons==["Sonarr rejected: Custom format score too low"]

class Response:
    def __init__(self,status,data):self.status_code=status;self.data=data;self.ok=200<=status<300
    def json(self):return self.data
class Session:
    def __init__(self,responses):self.responses=list(responses);self.calls=[]
    def get(self,*a,**k):self.calls.append(("GET",a,k));return self.responses.pop(0)
    def post(self,*a,**k):self.calls.append(("POST",a,k));return self.responses.pop(0)

def test_adapter_uses_only_interactive_get_then_exact_release_post():
    session=Session([Response(200,[release()]),Response(201,{})]);client=SonarrClient("http://sonarr","secret",session=session)
    assert len(client.search_season_releases(7,2))==1
    body={"guid":"g","indexerId":5,"downloadClientId":9};client.grab_release(body)
    assert session.calls[0][0]=="GET" and session.calls[0][2]["params"]=={"seriesId":7,"seasonNumber":2}
    assert session.calls[1][0]=="POST" and session.calls[1][2]["json"]==body
    assert all("command" not in call[1][0] for call in session.calls)

def test_adapter_surfaces_bounded_sonarr_rejection_reason():
    client=SonarrClient("http://sonarr","secret",session=Session([Response(400,[{"errorMessage":"Release was rejected by indexer"}])]))
    with pytest.raises(SonarrResponseError,match="Release was rejected by indexer"):client.grab_release({"guid":"g","indexerId":5,"downloadClientId":9})

def test_episode_search_regression_remains_exact_command():
    session=Session([Response(201,{"id":42,"name":"EpisodeSearch","status":"queued"})]);SonarrClient("http://s","k",session=session).search_episodes([1])
    assert session.calls[0][2]["json"]=={"name":"EpisodeSearch","episodeIds":[1]}
