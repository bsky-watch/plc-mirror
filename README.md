# Clone repo, cd to `$REPO_HOME/indexer`

docker compose up -d --build


# Add PDSs

`docker compose exec -it postgres psql -U postgres -d bluesky`

```
insert into pds (host) values ('https://agaric.us-west.host.bsky.network'),
 ('https://amanita.us-east.host.bsky.network'),
 ('https://blewit.us-west.host.bsky.network'),
 ('https://boletus.us-west.host.bsky.network'),
 ('https://bsky.social'),
 ('https://chaga.us-west.host.bsky.network'),
 ('https://conocybe.us-west.host.bsky.network'),
 ('https://enoki.us-east.host.bsky.network'),
 ('https://hydnum.us-west.host.bsky.network'),
 ('https://inkcap.us-east.host.bsky.network'),
 ('https://lepista.us-west.host.bsky.network'),
 ('https://lionsmane.us-east.host.bsky.network'),
 ('https://maitake.us-west.host.bsky.network'),
 ('https://morel.us-east.host.bsky.network'),
 ('https://oyster.us-east.host.bsky.network'),
 ('https://porcini.us-east.host.bsky.network'),
 ('https://puffball.us-east.host.bsky.network'),
 ('https://russula.us-west.host.bsky.network'),
 ('https://shiitake.us-east.host.bsky.network'),
 ('https://shimeji.us-east.host.bsky.network'),
 ('https://verpa.us-west.host.bsky.network');
```