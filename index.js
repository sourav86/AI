'use strict';
const express = require('express');
const app = express();
const fs = require('fs');


app.use(express.static(__dirname + '/views')); // html
app.use(express.static(__dirname + '/public')); // js, css, images

const server = app.listen(process.env.PORT || 5000, () => {
  console.log('Express server listening on port %d in %s mode', server.address().port, app.settings.env);
});

const io = require('socket.io')(server);
io.on('connection', function(socket){
  console.log('a user connected');
});

app.get('/', (req, res) => {
  res.sendFile('index.html');
});

app.get('/audio', (req, res) => {
  res.writeHead(200,{'Content-Type':'audio/mp3'});
  console.log(req.headers);
  var rs = fs.createReadStream('dance.mp3');
  rs.pipe(res);
});

io.on('connection', function(socket) {
  socket.on('chat message', (text) => {
    console.log("voiced received" , text)
  });
  setTimeout(function(){

     socket.emit('play-audio');

  },5000);
});
