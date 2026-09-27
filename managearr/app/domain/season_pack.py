"""Strict, fail-closed models for Sonarr manual season-pack grabs."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib, json, re
from typing import Any

PROTOCOLS = ("usenet", "torrent")
CUTOFF_RE = re.compile(r"^Existing file meets cutoff: \S(?:.*\S)?\Z")
SEASON_TOKEN_RE = re.compile(r"(?i)(?<![A-Z0-9])S(\d{1,3})(?![A-Z0-9])")
EPISODE_TOKEN_RE = re.compile(r"(?i)S\d{1,3}E\d{1,4}")
DAILY_TOKEN_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}[. _-]\d{2}[. _-]\d{2}(?!\d)")

def strict_int(value: Any, *, positive=False):
    return isinstance(value, int) and not isinstance(value, bool) and (not positive or value > 0)

def normalize_protocol(value: Any):
    if isinstance(value, bool): return None
    if isinstance(value, int): return {1: "usenet", 2: "torrent"}.get(value)
    if isinstance(value, str) and value.lower() in PROTOCOLS: return value.lower()
    return None

@dataclass(frozen=True)
class SeasonInventory:
    series_id: int
    season_number: int
    series_title: str
    episode_ids: tuple[int, ...]
    @classmethod
    def parse(cls, series, episodes, series_id, season_number):
        if not strict_int(series_id, positive=True) or not strict_int(season_number, positive=True):
            return None, "series_id and season_number must be positive integers; specials are not supported"
        if not isinstance(series, dict) or not strict_int(series.get("id"),positive=True) or series.get("id") != series_id:
            return None, "Sonarr returned malformed or mismatched series data"
        if series.get("seriesType") != "standard": return None, "daily and anime series are not supported for strict season packs"
        title=series.get("title")
        if not isinstance(title,str) or not title or len(title)>255 or not isinstance(episodes,list): return None,"Sonarr returned malformed series or episode data"
        for episode in episodes:
            season=episode.get("seasonNumber") if isinstance(episode,dict) else None
            if (not isinstance(episode,dict) or not strict_int(episode.get("id"),positive=True)
                    or not isinstance(season,int) or isinstance(season,bool) or season<0
                    or not strict_int(episode.get("episodeNumber"),positive=True)):
                return None,"Sonarr episode inventory is malformed or ambiguous"
        rows=[e for e in episodes if e["seasonNumber"]==season_number]
        if not rows:return None,"Sonarr has no authoritative episodes for the requested season"
        ids,numbers=[],[]
        for episode in rows:
            eid,enum=episode.get("id"),episode.get("episodeNumber")
            if not strict_int(eid,positive=True) or not strict_int(enum,positive=True):return None,"target-season episode inventory is malformed or ambiguous"
            ids.append(eid);numbers.append(enum)
        if len(ids)!=len(set(ids)) or len(numbers)!=len(set(numbers)):return None,"target-season episode inventory is duplicated or ambiguous"
        return cls(series_id,season_number,title,tuple(sorted(ids))),None

@dataclass(frozen=True)
class ReleaseDecision:
    guid:str; indexer_id:int; title:str; protocol:str; weight:int; episode_ids:tuple[int,...]; quality:dict; languages:list; cutoff_override:bool; decision_binding:dict
    @property
    def fingerprint(self):
        raw=json.dumps(self.decision_binding,sort_keys=True,separators=(",",":"),ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()
    def safe_dict(self):
        return {"fingerprint":self.fingerprint,"title":self.title,"protocol":self.protocol,"indexer_id":self.indexer_id,"episode_count":len(self.episode_ids),"cutoff_override":self.cutoff_override,"release_weight":self.weight}

def _quality_ok(value):
    if not isinstance(value,dict):return False
    q,r=value.get("quality"),value.get("revision")
    return isinstance(q,dict) and strict_int(q.get("id"),positive=True) and isinstance(q.get("name"),str) and bool(q["name"]) and isinstance(r,dict) and strict_int(r.get("version")) and strict_int(r.get("real")) and isinstance(r.get("isRepack"),bool)

def evaluate_release(raw,inventory,protocol,allow_cutoff,*,download_client_id=None,routing_evidence=None):
    reasons=[]
    if not isinstance(raw,dict):return None,["release is not an object"]
    if raw.get("fullSeason") is not True:reasons.append("not a confirmed full-season release")
    if not strict_int(raw.get("mappedSeriesId"),positive=True) or raw.get("mappedSeriesId")!=inventory.series_id:reasons.append("mapped series does not match")
    if not strict_int(raw.get("mappedSeasonNumber"),positive=True) or raw.get("mappedSeasonNumber")!=inventory.season_number:reasons.append("mapped season does not match")
    title=raw.get("title")
    if not isinstance(title,str) or not title or len(title)>512:reasons.append("release title is malformed")
    else:
        seasons={int(x) for x in SEASON_TOKEN_RE.findall(title)}
        if seasons!={inventory.season_number}:reasons.append("title does not identify exactly the requested season")
        if EPISODE_TOKEN_RE.search(title) or DAILY_TOKEN_RE.search(title):reasons.append("title is episode/daily ambiguous")
    actual_protocol=normalize_protocol(raw.get("protocol"))
    if actual_protocol!=protocol:reasons.append("release protocol does not match configured protocol")
    if raw.get("downloadAllowed") is not True:reasons.append("Sonarr does not allow this download")
    guid,indexer=raw.get("guid"),raw.get("indexerId")
    if not isinstance(guid,str) or not guid or len(guid)>1024:reasons.append("release guid is malformed")
    if not strict_int(indexer,positive=True):reasons.append("release indexer id is malformed")
    mapped=raw.get("mappedEpisodeInfo");mapped_ids=[]
    if not isinstance(mapped,list) or not mapped:reasons.append("mapped episode information is missing")
    else:
        nums=[]
        for entry in mapped:
            if not isinstance(entry,dict) or not strict_int(entry.get("id"),positive=True) or not strict_int(entry.get("seasonNumber"),positive=True) or entry.get("seasonNumber")!=inventory.season_number or not strict_int(entry.get("episodeNumber"),positive=True):
                reasons.append("mapped episode information is malformed or cross-season");break
            mapped_ids.append(entry["id"]);nums.append(entry["episodeNumber"])
        if len(mapped_ids)!=len(set(mapped_ids)) or len(nums)!=len(set(nums)):reasons.append("mapped episodes are duplicated or multi-episode ambiguous")
        if set(mapped_ids)!=set(inventory.episode_ids):reasons.append("release is not authoritatively complete for the season")
    quality,languages=raw.get("quality"),raw.get("languages")
    if not _quality_ok(quality):reasons.append("quality model is malformed")
    if not isinstance(languages,list) or any(not isinstance(x,dict) or not strict_int(x.get("id"),positive=True) or not isinstance(x.get("name"),str) or not x.get("name") for x in languages):reasons.append("language model is malformed")
    approved,rejected,temp,rejections=raw.get("approved"),raw.get("rejected"),raw.get("temporarilyRejected"),raw.get("rejections")
    override=False
    if approved is True and rejected is False and temp is False and rejections==[]:pass
    elif allow_cutoff and approved is False and rejected is True and temp is False and isinstance(rejections,list) and rejections and all(isinstance(x,str) and CUTOFF_RE.fullmatch(x) for x in rejections):override=True
    else:
        if isinstance(rejections,list) and rejections:reasons.extend([f"Sonarr rejected: {x}" if isinstance(x,str) and len(x)<=300 else "Sonarr returned a malformed rejection reason" for x in rejections])
        else:reasons.append("Sonarr did not approve the release")
    if reasons:return None,reasons
    weight=raw.get("releaseWeight")
    if not strict_int(weight):weight=2**31-1
    mapped_normalized=sorted(({"id":x["id"],"season_number":x["seasonNumber"],"episode_number":x["episodeNumber"]} for x in mapped),key=lambda x:(x["episode_number"],x["id"]))
    override_payload=None
    if override:
        override_payload={"shouldOverride":True,"seriesId":inventory.series_id,"episodeIds":sorted(mapped_ids),"quality":quality,"languages":languages}
    binding={
        "identity":{"guid":guid,"title":title,"indexer_id":indexer,"series_id":inventory.series_id,"season_number":inventory.season_number},
        "release":{"protocol":actual_protocol,"weight":weight,"quality":quality,"languages":languages,"mapped_episodes":mapped_normalized},
        "sonarr_decision":{"full_season":raw.get("fullSeason"),"download_allowed":raw.get("downloadAllowed"),"approved":approved,"rejected":rejected,"temporarily_rejected":temp,"rejections":rejections,"cutoff_override":override,"override_payload":override_payload},
        "routing":{"download_client_id":download_client_id,"evidence":routing_evidence},
    }
    return ReleaseDecision(guid,indexer,title,actual_protocol,weight,tuple(sorted(mapped_ids)),quality,languages,override,binding),[]
