const request = require('request');
const fs = require('fs');
const path = require('path');
const btoa = require('btoa');
const process = require('process');
const Promise = require('bluebird');

var mysql = require('mysql');
const util = require('util');

const credentials = JSON.parse(fs.readFileSync(__dirname + '/credentials.json'));
const redirect_uri = 'http://localhost:8888/callback';

var con = mysql.createConnection({
    host: "spotify-listening-history-022724.cnwmw4kealbs.us-east-1.rds.amazonaws.com",
    port: "3306",
    user: "admin",
    password: "WearyBunny12916",
    database: "spotify_listening_history_022724"
  });

con.connect(function(err) {
    if (err) throw err;
    console.log("Connected!");
  });

const readFileAsync = util.promisify(fs.readFile);
const queryAsync = util.promisify(con.query.bind(con));

if (fs.existsSync(__dirname + '/authorization.json')) {
  var authorization = JSON.parse(fs.readFileSync(__dirname + '/authorization.json'));
  saveRecentTracks(authorization);
} else {
  authorizeWithCode(
    credentials.client_id,
    credentials.client_secret,
    credentials.authorization_code,
    redirect_uri
  ).then(function(tokens) {
    fs.writeFileSync(
      __dirname + '/authorization.json',
      JSON.stringify(tokens, null, 2)
    );
    saveRecentTracks(tokens);
  }).catch(function(error) {
    console.error(error);
    process.exit(0);
  });
}


async function saveRecentTracks(authorization) {
    try {
        const newAuthorization = await authorizeWithRefreshToken(
            credentials.client_id,
            credentials.client_secret,
            authorization.refresh_token
        );

        var latestTimestamp;
        var directory = __dirname + '/data/';

        try {
            const track = await readFirstTrackFrom(directory + "latest.json");
            latestTimestamp = track.played_at;
        } catch (error) {
            console.error(error);
            // Handle error if needed
            return process.exit(1);
        }

        const tracks = await getRecentTracks(newAuthorization.access_token, latestTimestamp);

        if (tracks.items.length === 0) {
            console.log('No new songs have played since ' + new Date(latestTimestamp).toLocaleString());
        } else {
            // Insert tracks directly into MySQL tables
            const mostRecentTrack = tracks.items[0];
            fs.writeFileSync(directory + "latest.json", JSON.stringify(mostRecentTrack, null, 2));

            for (const track of tracks.items) {
                const simplifiedTrack = {
                    artist: track.track.artists.map(artist => artist.name).join(', '),
                    songName: track.track.name,
                    trackId: track.track.id,
                    playTime: track.played_at,
                    duration: track.track.duration_ms,
                    albumName: track.track.album.name
                };

                try {
                    // Insert into MySQL 'songs' table
                    await queryAsync(
                        'INSERT INTO songs (artist, songName, trackId, playTime, duration, albumName) VALUES (?, ?, ?, ?, ?, ?)',
                        Object.values(simplifiedTrack)
                    );

                    // Split the artists by comma and insert into 'split_songs' table
                    for (const artist of simplifiedTrack.artist.split(',')) {
                        const artist_track = {
                            artist: artist.trim(),
                            song: simplifiedTrack.songName,
                            trackId: simplifiedTrack.trackId,
                            playTime: simplifiedTrack.playTime,
                            duration: simplifiedTrack.duration,
                            albumName: simplifiedTrack.albumName
                        };

                        await queryAsync(
                            `INSERT INTO split_songs (artist, songName, trackId, playTime, duration, albumName) VALUES (?, ?, ?, ?, ?, ?)`,
                            Object.values(artist_track)
                        );
                    }

                    console.log('Inserted songs into MySQL tables.');
                } catch (error) {
                    if (error.code === 'ER_DATA_TOO_LONG') {
                        console.warn('Skipping row due to data too long:', simplifiedTrack);
                        continue; // Skip this iteration and move to the next row
                    }

                    console.error(error);
                    // Handle other errors if needed
                    return process.exit(1);
                }
            }
        }
    } catch (error) {
        console.error(error);
        // Handle other errors if needed
        return process.exit(1);
    } finally {
        process.exit(0);
    }
}

  

  function readFirstTrackFrom(file) {
    return new Promise(function(resolve, reject) {
      try {
        var text = fs.readFileSync(file, 'utf8');
        var content = JSON.parse(text);
  
        if (content && content.played_at && content.track) {
          resolve({
            played_at: content.played_at,
            track: {
              artist: content.track.artists.map(artist => artist.name).join(', '),
              songName: content.track.name,
              trackId: content.track.id,
              playTime: content.played_at,
              duration: content.track.duration_ms,
              albumName: content.track.album.name
            }
          });
        } else {
          // If the expected properties are not present, resolve with a default value
          reject(new Error('Invalid structure in latest.json'));
        }
      } catch (error) {
        // If an error occurs during file read or JSON parse, reject with the error
        reject(error);
      }
    });
  }
  
  
  function authorizeWithRefreshToken(client_id, client_secret, refresh_token) {
    var botaAuth = btoa(client_id + ":" + client_secret);
    return new Promise(function(resolve, reject) {
      request({
        url: "https://accounts.spotify.com/api/token",
        method: 'POST',
        form: {
          grant_type: 'refresh_token',
          refresh_token: refresh_token
        },
        headers: {
          'Authorization': "Basic " + botaAuth
        }
      }, function(e, response) {
        if (e) {
          reject(e);
          return;
        }
        resolve(JSON.parse(response.body));
      });
    });
  }
  
  function authorizeWithCode(client_id, client_secret, authorization_code, redirect_uri) {
    var botaAuth = btoa(client_id + ":" + client_secret);
    return new Promise(function(resolve, reject) {
      request({
        url: "https://accounts.spotify.com/api/token",
        method: 'POST',
        form: {
          grant_type: 'authorization_code',
          code: authorization_code,
          redirect_uri: redirect_uri
        },
        headers: {
          'Authorization': "Basic " + botaAuth
        }
      }, function(e, response) {
        if (e) {
          reject(e);
          return;
        }
        const body = JSON.parse(response.body);
        if (body.access_token) {
          resolve(body);
        } else {
          const err = new Error('Response does not have access_token');
          err.body = body;
          reject(err);
        }
      });
    });
  }
  
  function getRecentTracks(access_token, afterTimestamp) {
    return new Promise(function(resolve, reject) {
      console.log("Getting tracks after " + new Date(afterTimestamp).toLocaleString());
      const q = afterTimestamp ? {
        after: (new Date(afterTimestamp)).getTime(),
        limit: 50
      } : {
        limit: 50
      };
      request({
        url: "https://api.spotify.com/v1/me/player/recently-played",
        method: 'GET',
        qs: q,
        headers: {
          'Authorization': 'Bearer ' + access_token
        }
      }, function(e, response) {
        if (e) {
          reject(e);
          return;
        }
        resolve(JSON.parse(response.body));
      });
    });
  }
  
  
  // Close connections after processing
  process.on('exit', () => {
   con.end();
  });
  
