/*************************************
//
// beanstalknoded app
//
**************************************/

// express magic
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io').listen(server);
var device  = require('express-device');
var _ = require("underscore");
var net = require('net');
var BeanClientModule = require('./beanstalk_client');

var runningPortNumber = 8002;

// I need to access everything in '/public' directly
app.use(express.static(__dirname + '/public'));

//set the view engine
app.set('view engine', 'ejs');
app.set('views', __dirname +'/views');

app.use(device.capture());


// logs every request
app.use(function(req, res, next){
	// output every request in the array
	console.log({method:req.method, url: req.url, device: req.device});

	// goes onto the next function in line
	next();
});

app.get("/", function(req, res){
	res.render('index', {});
});


io.sockets.on('connection', function (socket) {

	io.sockets.emit('blast', {msg:"<span style=\"color:red !important\">someone connected</span>"});

	socket.on('blast', function(data, fn){
		console.log(data);
		io.sockets.emit('blast', {msg:data.msg});

		fn();//call the client back to clear out the field
	});

});














var proc = new BeanProcessorModule.BeanProcessor();


/*
 * Method executed when data is received from a socket
 */
function receiveData(socket, data)
{
    // find the client
    var _client = _.find(proc.bean_clients, function (obj) {
        return obj.socket == socket;
    });

    if (_client != null) {
        _client.dataReceived(data);
    }
}

var sockets = [];
/*
 * Method executed when a socket ends
 */
function closeSocket(socket) {
    var i = sockets.indexOf(socket);
    if (i != -1) {
        sockets.splice(i, 1);
    }
}

/*
 * Callback method executed when a new TCP socket is opened.
 */
function newSocket(socket) {
    sockets.push(socket);
    socket.write('Welcome to the Telnet server!\n');

    proc.bean_clients.push(new BeanClientModule.BeanClient(socket, bean_processor));

    socket.on('data', function(data) {
        receiveData(socket, data);
    });
    socket.on('end', function() {
        closeSocket(socket);
    });
}

// Create a new server and provide a callback for when a connection occurs
var telnet_server = net.createServer(newSocket);
telnet_server.listen(8003);


server.listen(runningPortNumber);

