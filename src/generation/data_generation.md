
### Offline Data Generation

1. User data:
   user_id gender  age  subscription_type           signup_ts
0        1      F   80           Standard 2025-12-17 16:19:38
1        2      F   74  Standard-With-Ads 2025-12-11 07:46:21
2        3      O   56           Standard 2026-03-24 15:43:32
3        4      M   25            Premium 2026-03-23 04:22:46
4        5      M   52           Standard 2026-02-10 07:37:29 

Total numbers of users generated:  100000 

2. Movie data: 
Before schema evolution: 
   movie_id      genre  runtime_seconds  language  release_year          created_at
0         1     Action            10055  Japanese          1995 2026-01-04 14:56:31
1         2  Animation             6589   Chinese          2010 2026-01-25 06:19:08
2         3      Drama             8390    French          2005 2026-01-11 09:27:25
3         4    Fantasy             8825    French          2007 2026-01-01 08:31:40
4         5      Drama             7056   Chinese          1995 2026-01-23 06:02:09 

After schema evolution: 
   movie_id      genre  runtime_seconds    language  release_year          created_at  country
0     83331  Animation             3839     English          1984 2026-06-24 05:29:51       UK
1     83332      Drama            10261    Japanese          2000 2026-06-09 16:13:41    Japan
2     83333      Drama             2186  Vietnamese          1995 2026-06-28 20:24:19  Vietnam
3     83334      Drama             5485     Spanish          2010 2026-06-15 10:19:20    Spain
4     83335      Drama             3018      French          1984 2026-06-17 18:12:49   France 

Total number of movies generated: 100000 

3. Playback data:
   playback_id  user_id  movie_id            click_ts            start_ts  completion_rate  duration_watched_seconds              end_ts playback_date
0            1    74383     97144 2026-05-21 17:16:38 2026-05-21 17:16:51         0.122698                       568 2026-05-21 17:26:19    2026-05-21
1            2    49640     71398 2025-12-13 20:40:39 2025-12-13 20:40:51         0.151840                      1110 2025-12-13 20:59:21    2025-12-13
2            3    54749     71601 2026-05-14 07:15:54 2026-05-14 07:16:05         0.079798                       606 2026-05-14 07:26:11    2026-05-14
3            4    90219     22061 2025-12-17 15:24:22 2025-12-17 15:24:32         0.472673                      1682 2025-12-17 15:52:34    2025-12-17
4            5    77826     55648 2026-05-11 23:56:07 2026-05-11 23:56:19         0.890601                      4206 2026-05-12 01:06:25    2026-05-11 

Total number of playback transactions generated: 105000 

4. Ratings data:
   rating_id  user_id  movie_id  rating           rating_ts rating_date
0          1    49236     14567       5 2026-03-08 14:05:39  2026-03-08
1          2    41568      6148       3 2026-04-21 17:11:31  2026-04-21
2          3    17103     96118       2 2026-06-05 14:20:17  2026-06-05
3          4    94392     91794       4 2026-04-18 11:55:51  2026-04-18
4          5    96958     36471       1 2026-05-30 06:41:53  2026-05-30 

Total number of ratings generated: 50000

5. Payments data:
   payment_id  user_id payment_method payment_status          payment_ts  amount payment_date
0           1    18983     Debit Card         Failed 2026-04-15 12:33:19   19.99   2026-04-15
1           2    74937    Credit Card        Success 2026-04-01 23:10:52   26.99   2026-04-01
2           3    18694     Debit Card        Success 2026-05-29 07:21:40   19.99   2026-05-29
3           4    57966     Debit Card         Failed 2026-04-14 21:02:33    8.99   2026-04-14
4           5    24686    Credit Card        Pending 2026-02-11 15:19:49   19.99   2026-02-11 

Total number of payments generated: 50000


### Streaming Data Generation

Generated events for minute offset 300 (2026-07-06 19:00:00). Total: 88100
Generated events for minute offset 360 (2026-07-06 18:00:00). Total: 94100
Generated events for minute offset 420 (2026-07-06 17:00:00). Total: 100100
Generated events for minute offset 480 (2026-07-06 16:00:00). Total: 106100
Generated events for minute offset 540 (2026-07-06 15:00:00). Total: 112100
Generated events for minute offset 600 (2026-07-06 14:00:00). Total: 118100
Generated events for minute offset 660 (2026-07-06 13:00:00). Total: 124100
Generated events for minute offset 720 (2026-07-06 12:00:00). Total: 188100
Generated events for minute offset 780 (2026-07-06 11:00:00). Total: 194100
Generated events for minute offset 840 (2026-07-06 10:00:00). Total: 200100
Generated events for minute offset 900 (2026-07-06 09:00:00). Total: 206100
Generated events for minute offset 960 (2026-07-06 08:00:00). Total: 212100
Generated events for minute offset 1020 (2026-07-06 07:00:00). Total: 218100
Generated events for minute offset 1080 (2026-07-06 06:00:00). Total: 224100
Generated events for minute offset 1140 (2026-07-06 05:00:00). Total: 230100
Generated events for minute offset 1200 (2026-07-06 04:00:00). Total: 236100
Generated events for minute offset 1260 (2026-07-06 03:00:00). Total: 242100
Generated events for minute offset 1320 (2026-07-06 02:00:00). Total: 248100
Generated events for minute offset 1380 (2026-07-06 01:00:00). Total: 254100

 Streaming event data: 
        event_id    event_type             event_ts           created_ts  user_id         session_id  movie_id      playback_id    playback_start_ts  current_playback_offset_seconds
0  event_1439_11         start  2026-07-06T00:01:00  2026-07-06T00:01:00    11176  user_11176_sess_1   54312.0  playback_207635  2026-07-06T00:01:03                           3594.0
1  event_1439_29       impress  2026-07-06T00:01:00  2026-07-06T00:01:00    58155  user_58155_sess_1       NaN             None                 None                              NaN
2  event_1439_67       impress  2026-07-06T00:01:00  2026-07-06T00:01:00    15922  user_15922_sess_1       NaN             None                 None                              NaN
3  event_1439_91          stop  2026-07-06T00:01:00  2026-07-06T00:01:00    87993  user_87993_sess_1   99419.0  playback_207696  2026-07-06T00:01:05                           2468.0
4   event_1439_9  fast-forward  2026-07-06T00:01:01  2026-07-06T00:01:01    61963  user_61963_sess_1   99024.0  playback_207633  2026-07-06T00:01:03                           3584.0

Generated 265200 streaming events.
Generated 265200 streaming events and saved to ../../data/raw/streaming_2/streaming_events.jsonl