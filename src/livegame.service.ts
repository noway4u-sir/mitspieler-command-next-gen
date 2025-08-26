import { HttpService } from "@nestjs/axios";
import { flatten, Inject, Injectable } from "@nestjs/common";
import { DeepLolSummonerCachedResponse, Summoner } from "./models/deeplol";
import { catchError, combineLatest, filter, from, map, mergeMap, Observable, of, switchMap, tap, throwError, toArray, withLatestFrom } from "rxjs";
import { DeepLolLiveGame } from "./models/deeplol_game";
import { calculateAverageRank } from "./average-rank-calculator";
import { Cache, CACHE_MANAGER } from "@nestjs/cache-manager";
import { RiotVersionService } from "./riot-version.service";
import { DeeplolStreamerPro } from "./models/deeplol_streamer_pro";

export interface LivegameResponse {
  averageRank?: {
    tier: string;
    rank: string;
    leaguePoints: number;
  };
  knownPlayers: {
    name: string;
    combinedName: string;
    team?: string;
    championName: string;
  }[];

}


const RegionToPlatformId = {
  'euw': 'EUW1',
  'eune': 'EUN1',
  'na': 'NA1',
  'br': 'BR1',
  'lan': 'LA1',
  'las': 'LA2',
  'oce': 'OC1',
  'tr': 'TR1',
  'ru': 'RU',
  'jp': 'JP1',
  'kr': 'KR',
}

export class GameNotFoundError extends Error {
  constructor(public summonerName: string) {
    super(`Summoner ${summonerName} does not exist`);
  }
}

@Injectable()
export class LivegameService {

  constructor(
    private readonly http: HttpService,
    @Inject(CACHE_MANAGER) private cacheManager: Cache,
    private readonly riotVersion: RiotVersionService
  ) { }

  getLivegameForStreamer(name: string, streamerOrPro: 'streamer' | 'pro') {
    return this.getAccountsForStreamer(name, streamerOrPro).pipe(
      switchMap(streamer => {
        if (!streamer || streamer?.status === '') {
          return of(null);
        }
        const active = streamer.account_list.find(it => it.live === 1);

        return this.getLivegameBySummonerId(active.platform_id, active.puu_id, true);
      }),

      switchMap(livegame => {
        if (!livegame) {
          return throwError(() => new GameNotFoundError(name));
        }
        const summoners = this.getSummonerInfosByIds(livegame.platformId, livegame.participants.map(player => player.summonerId), true).pipe(
          map(summoners => {
            console.log(summoners)
            return summoners.filter(it => !!it)

          })
        )
        return combineLatest([of(livegame), summoners, from(this.riotVersion.getLatestChampions())])
      }),
      map(([livegame, summoners, champs]) => {
        const knownPlayers = summoners
          .filter(it => !!it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.status)
          .map(
            it => {
              const participant = livegame.participants.find(p => p.summonerId === it.summoner.summoner_basic_info_dict.summoner_id)
              if (!participant) {
                return null;
              }
              return {
                name: it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.name,
                combinedName: `${it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.pro_team_al ?? ''} ${it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.name}`.trimStart(),
                championName: champs.get(participant.championId)?.name + ''
              }

            }
          ).filter(it => !!it)
        const averageRank = calculateAverageRank(summoners);
        return {
          knownPlayers: knownPlayers.filter(it => !!it),
          averageRank,
        }
      }),
      tap(response => {
        console.log(response)
        this.cacheManager.set(`${name.toLowerCase()}-game`, response, 60 * 5 * 1000)
      })
    )

  }

  getAccountsForStreamer(name: string, streamerOrPro: 'streamer' | 'pro') {
    const url = `https://b2c-api-cdn.deeplol.gg/summoner/strm_pro_info?name=${name}&status=${streamerOrPro}`
    const cacheKey = `${name}-${streamerOrPro}-accounts`
    return from(this.cacheManager.get<DeeplolStreamerPro>(cacheKey)).pipe(
      switchMap(cached => {
        if (cached) {
          return of(cached);
        }

        return this.http.get<DeeplolStreamerPro>(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        }).pipe(
          map(response => response.data),
          tap(res => this.cacheManager.set(cacheKey, res, 60 * 3 * 1000)),
          catchError(error => {
            console.error(error);
            if (error.response.status === 404) {
              return of(null);
            }
            return throwError(error);
          }))
      })
    )
  }


  getLivegameInfoBySummoner(region: string, summonerName: string,): Observable<LivegameResponse> {

    // Look into the cache first
    return from(
      this.cacheManager.get<LivegameResponse>(`${region.toLowerCase()}-${summonerName.toLowerCase()}`)
    ).pipe(
      switchMap(cached => {
        if (cached) {
          return of(cached);
        }

        return this.getLivegameBySummoner(region, summonerName).pipe(
          switchMap(livegame => {
            if (!livegame) {
              return throwError(() => new GameNotFoundError(summonerName));
            }
            const summoners = this.getSummonerInfosByIds(region, livegame.participants.map(player => player.summonerId)).pipe(
              map(summoners => {
                console.log(summoners)
                return summoners.filter(it => !!it)

              })
            )
            return combineLatest([of(livegame), summoners, from(this.riotVersion.getLatestChampions())])
          }),
          map(([livegame, summoners, champs]) => {
            const knownPlayers = summoners
              .filter(it => !!it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.status)
              .map(
                it => {
                  const participant = livegame.participants.find(p => p.summonerId === it.summoner.summoner_basic_info_dict.summoner_id)
                  if (!participant) {
                    return null;
                  }
                  return {
                    name: it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.name,
                    combinedName: `${it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.pro_team_al ?? ''} ${it.summoner.summoner_basic_info_dict.pro_streamer_info_dict.name}`.trimStart(),
                    championName: champs.get(participant.championId)?.name + ''
                  }

                }
              ).filter(it => !!it)
            const averageRank = calculateAverageRank(summoners);
            return {
              knownPlayers: knownPlayers.filter(it => !!it),
              averageRank,
            }
          }),
          tap(response => {
            this.cacheManager.set(`${region.toLowerCase()}-${summonerName.toLowerCase()}`, response, 60 * 5 * 1000)
          })
        )

      })
    )
  }

  getLivegameBySummoner(region: string, summonerName: string) {
    return this.getSummonerInfoByName(region, summonerName).pipe(
      switchMap(summoner => {
        if (!summoner) {
          return of(null);
        }
        return this.getLivegameBySummonerId(region, summoner.summoner_basic_info_dict.puu_id);
      }),

    )
  }

  getSummonerInfosByIds(region: string, summonerIds: string[], isPlatform = false): Observable<DeepLolSummonerCachedResponse[]> {
    return from(summonerIds).pipe(
      mergeMap(summonerId => this.getSummonerInfoById(region, summonerId, isPlatform), 10),
      toArray()
    )
  }

  getSummonerInfoById(region: string, summonerId: string, isPlatform = false): Observable<DeepLolSummonerCachedResponse> {
    const cacheKey = `${region.toLowerCase()}-${summonerId.toLowerCase()}-cached`
    const platformId = isPlatform ? region : RegionToPlatformId[region] ?? 'EUW1';
    const url = `https://b2c-api-cdn.deeplol.gg/ingame/summoner-cached?summoner_id=${summonerId}&platform_id=${platformId}`
    return from(
      this.cacheManager.get<DeepLolSummonerCachedResponse>(cacheKey)
    ).pipe(
      switchMap(cached => {
        if (cached) return of(cached);


        return this.http.get<DeepLolSummonerCachedResponse>(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        }).pipe(
          map(response => response.data),
          tap(res => this.cacheManager.set(cacheKey, res, 60 * 240 * 1000)),
          catchError(error => {
            console.error(error);
            if (error.response.status === 404) {
              return of(null);
            }
            return throwError(error);
          }))
      })
    )
  }

  getLivegameBySummonerId(region: string, summonerId: string, isPlatform = false): Observable<DeepLolLiveGame> {
    const cacheKey = `${region.toLowerCase()}-${summonerId.toLowerCase()}-live`
    const platformId = isPlatform ? region : RegionToPlatformId[region] ?? 'EUW1';
    const url = `https://ingame-basic-test.deeplol-gg.workers.dev/`
    const formData = new FormData();
    formData.append('platform_id', platformId);
    formData.append('puu_id', summonerId);
    return from(
      this.cacheManager.get<DeepLolLiveGame>(cacheKey)
    ).pipe(
      switchMap(cached => {
        if (cached) return of(cached);
        return this.http.post(url, formData, {
          headers: {
            'Content-Type': 'multipart/form-data',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        }).pipe(
          tap(response => {
            console.log("Status Code", response.status)
            console.log("Response Body", response.data)
          }),
          map(response => {
            if (response.data?.status?.status_code === 404) {
              console.log("404")
              return null;
            }
            return response.data;
          }),
          tap(res => this.cacheManager.set(cacheKey, res, 60 * 5 * 1000)),
          catchError(error => {
            if (error.response.status === 404) {
              return of(null);
            }
            return throwError(err => error);
          })
        )
      }))

  }


  getSummonerInfoByName(region: string, summonerName: string): Observable<Summoner> {
    const [riotIdName, riotIdTag] = summonerName.split('-');
    const platformId = RegionToPlatformId[region] ?? 'EUW1';
    const url = `https://b2c-api-cdn.deeplol.gg/summoner/summoner?riot_id_name=${riotIdName}&riot_id_tag_line=${riotIdTag}&platform_id=${platformId}`
    const cacheKey = `${region.toLowerCase()}-${summonerName.toLowerCase()}-cached`

    return from(
      this.cacheManager.get<Summoner>(cacheKey)
    ).pipe(
      switchMap(cached => {
        if (cached) return of(cached);
        return this.http.get<Summoner>(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        }).pipe(

          map(response => response.data),
          catchError(error => {

            console.error(error);
            if (error.response.status === 404) {
              return of(null);
            }
            return throwError(error);
          })
        );
      })
    )

  }


}
